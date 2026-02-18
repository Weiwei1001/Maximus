#!/usr/bin/env python3
"""
Run Maximus benchmarks with GPU metrics sampling.

Runs each query individually while sampling GPU metrics (power, utilization,
memory, PCIe) via nvidia-smi at configurable intervals. Results are written
to CSV in real-time after each query.

Usage:
    python run_metrics.py --maximus-dir /path/to/Maximus --data-dir /path/to/data --n-reps 3
    python run_metrics.py --maximus-dir /path/to/Maximus --data-dir /path/to/data --sample-interval 50
"""
import argparse
import csv
import os
import re
import subprocess
import threading
import time
from pathlib import Path


TPCH_QUERIES = "q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22"
H2O_QUERIES = "q1,q2,q3,q4,q5,q6,q7,q9,q10"
CLICK_QUERIES = "q3,q6,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q19,q21,q22,q23,q24,q25,q26,q30,q31,q32,q33,q34,q35"

BENCHMARK_CONFIGS = {
    "tpch": {
        "queries": TPCH_QUERIES,
        "scales": ["sf1", "sf2", "sf10", "sf20"],
        "data_subdir": "tpch",
    },
    "h2o": {
        "queries": H2O_QUERIES,
        "scales": ["sf1", "sf2", "sf3", "sf4"],
        "data_subdir": "h2o",
    },
    "clickbench": {
        "queries": CLICK_QUERIES,
        "scales": ["sf10", "sf20"],
        "data_subdir": "clickbench",
    },
}

SAMPLE_FIELDS = ["benchmark", "scale", "query", "time_offset_ms", "power_w", "gpu_util_pct", "mem_used_mb", "pcie_gen"]
TIMING_FIELDS = ["benchmark", "scale", "query", "min_ms", "avg_ms", "reps"]


class GPUSampler:
    """Background GPU metrics sampler using nvidia-smi."""

    def __init__(self, interval_ms: int = 50):
        self.interval_s = interval_ms / 1000.0
        self.samples = []
        self._stop = threading.Event()
        self._thread = None
        self._meta = {}

    def start(self, **meta):
        """Start sampling. Meta fields are attached to each sample."""
        self.samples = []
        self._stop.clear()
        self._meta = meta
        self._t0 = time.time()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop sampling and return collected samples."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        return self.samples

    def _loop(self):
        while not self._stop.is_set():
            try:
                result = subprocess.run(
                    ["nvidia-smi",
                     "--query-gpu=power.draw,utilization.gpu,memory.used,pcie.link.gen.current",
                     "--format=csv,noheader,nounits"],
                    capture_output=True, text=True, timeout=2,
                )
                if result.returncode == 0:
                    parts = result.stdout.strip().split(",")
                    if len(parts) >= 4:
                        sample = {
                            **self._meta,
                            "time_offset_ms": round((time.time() - self._t0) * 1000, 2),
                            "power_w": float(parts[0].strip()),
                            "gpu_util_pct": float(parts[1].strip()),
                            "mem_used_mb": float(parts[2].strip()),
                            "pcie_gen": parts[3].strip(),
                        }
                        self.samples.append(sample)
            except Exception:
                pass
            self._stop.wait(self.interval_s)


def build_env(maximus_dir: Path):
    """Build environment variables for maxbench."""
    env = os.environ.copy()
    build_dir = maximus_dir / "build"
    lib_paths = [
        "/arrow_install/lib",
        str(build_dir / "src" / "maximus"),
        str(build_dir / "third_party"),
    ]
    for candidate in ["/venv/maximus_gpu/lib", os.path.expanduser("~/miniforge3/envs/maximus_gpu/lib")]:
        if Path(candidate).exists():
            lib_paths.append(candidate)
    env["LD_LIBRARY_PATH"] = ":".join(lib_paths) + ":" + env.get("LD_LIBRARY_PATH", "")
    return env


def run_maxbench(maximus_dir: Path, benchmark: str, queries: str, data_path: str,
                 n_reps: int, storage_device: str, env: dict):
    """Run maxbench and return (stdout+stderr, elapsed_seconds)."""
    maxbench = str(maximus_dir / "build" / "benchmarks" / "maxbench")
    cmd = [
        maxbench,
        f"--benchmark={benchmark}",
        f"--queries={queries}",
        "--device=gpu",
        f"--storage_device={storage_device}",
        "--engines=maximus",
        f"--n_reps={n_reps}",
        f"--path={data_path}",
    ]
    t0 = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, env=env,
                            cwd=str(maximus_dir / "build"), timeout=3600)
    return result.stdout + result.stderr, time.time() - t0


def extract_timings(output: str):
    """Extract per-query timings: list of (query, reps_list, min_ms, avg_ms)."""
    results = []
    queries = re.findall(r'QUERY (q\d+\w*)', output)
    timing_blocks = re.findall(r'MAXIMUS TIMINGS \[ms\]:\s*([\d,\s\t]+)', output)
    stat_blocks = re.findall(r'MAXIMUS STATS: MIN = (\d+) ms;\s*MAX = (\d+) ms;\s*AVG = (\d+) ms', output)

    for i, qname in enumerate(queries):
        reps = []
        if i < len(timing_blocks):
            reps = [int(x.strip()) for x in timing_blocks[i].split(',') if x.strip()]
        min_ms = int(stat_blocks[i][0]) if i < len(stat_blocks) else (min(reps) if reps else 0)
        avg_ms = int(stat_blocks[i][2]) if i < len(stat_blocks) else (sum(reps) // len(reps) if reps else 0)
        results.append((qname, reps, min_ms, avg_ms))
    return results


def append_csv(filepath: Path, rows: list, fieldnames: list):
    """Append rows to CSV, creating with header if file doesn't exist."""
    write_header = not filepath.exists()
    with open(filepath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def run_benchmark_metrics(benchmark: str, config: dict, maximus_dir: Path, data_dir: Path,
                          output_dir: Path, n_reps: int, storage_device: str,
                          sample_interval: int, env: dict):
    """Run a benchmark with per-query GPU metrics sampling."""
    samples_path = output_dir / f"{benchmark}_metrics_samples.csv"
    timings_path = output_dir / f"{benchmark}_metrics_timings.csv"

    # Remove old results
    for p in [samples_path, timings_path]:
        if p.exists():
            p.unlink()

    sampler = GPUSampler(interval_ms=sample_interval)
    query_list = config["queries"].split(",")

    for sf in config["scales"]:
        data_path = str(data_dir / config["data_subdir"] / sf)
        if not Path(data_path).exists():
            print(f"  [{benchmark} {sf}] Data not found at {data_path}, skipping")
            continue

        print(f"\n  [{benchmark} {sf}] Metrics: {len(query_list)} queries x {n_reps} reps")

        for q in query_list:
            print(f"    {q}...", end=" ", flush=True)

            sampler.start(benchmark=benchmark, scale=sf, query=q)
            try:
                output, elapsed = run_maxbench(maximus_dir, benchmark, q, data_path,
                                               n_reps, storage_device, env)
                timings = extract_timings(output)
                if timings:
                    for qname, reps, min_ms, avg_ms in timings:
                        reps_str = ",".join(str(r) for r in reps)
                        # Real-time append timing
                        append_csv(timings_path, [{
                            "benchmark": benchmark, "scale": sf, "query": qname,
                            "min_ms": min_ms, "avg_ms": avg_ms, "reps": reps_str,
                        }], TIMING_FIELDS)
                        print(f"min={min_ms}ms", end=" ", flush=True)
                else:
                    print("(no timing)", end=" ", flush=True)
            except subprocess.TimeoutExpired:
                print("TIMEOUT", end=" ", flush=True)
            except Exception as e:
                print(f"ERROR: {e}", end=" ", flush=True)

            samples = sampler.stop()
            # Real-time append samples
            if samples:
                append_csv(samples_path, samples, SAMPLE_FIELDS)
            print(f"({len(samples)} samples)")


def main():
    parser = argparse.ArgumentParser(description="Run Maximus benchmarks with GPU metrics sampling")
    parser.add_argument("--maximus-dir", type=str, required=True, help="Path to Maximus repository")
    parser.add_argument("--data-dir", type=str, required=True,
                        help="Base data directory (contains tpch/, h2o/, clickbench/ subdirs)")
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory (default: data-dir)")
    parser.add_argument("--n-reps", type=int, default=3, help="Number of repetitions (default: 3)")
    parser.add_argument("--sample-interval", type=int, default=50, help="GPU sampling interval in ms (default: 50)")
    parser.add_argument("--storage-device", type=str, default="gpu", choices=["cpu", "gpu"],
                        help="Storage device (default: gpu)")
    parser.add_argument("--benchmarks", type=str, nargs="+", default=["tpch", "h2o", "clickbench"],
                        choices=["tpch", "h2o", "clickbench"], help="Benchmarks to run")
    args = parser.parse_args()

    maximus_dir = Path(args.maximus_dir)
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir) if args.output_dir else data_dir

    if not (maximus_dir / "build" / "benchmarks" / "maxbench").exists():
        print("ERROR: maxbench not found. Please build Maximus first.")
        return

    env = build_env(maximus_dir)

    print("=" * 60)
    print(f"  Maximus GPU Metrics Benchmark")
    print(f"  Start: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Storage device: {args.storage_device}")
    print(f"  Repetitions: {args.n_reps}")
    print(f"  Sample interval: {args.sample_interval}ms")
    print(f"  Benchmarks: {args.benchmarks}")
    print("=" * 60)

    for bench in args.benchmarks:
        config = BENCHMARK_CONFIGS[bench]
        print(f"\n{'#' * 60}")
        print(f"#  {bench.upper()} Metrics")
        print(f"{'#' * 60}")
        run_benchmark_metrics(bench, config, maximus_dir, data_dir, output_dir,
                              args.n_reps, args.storage_device, args.sample_interval, env)

    print(f"\n{'=' * 60}")
    print(f"  Metrics complete: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Results in: {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
