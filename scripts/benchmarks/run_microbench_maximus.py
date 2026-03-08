#!/usr/bin/env python3
"""Run microbench queries via Maximus (GPU query engine).

Runs each microbench query plan through maxbench, measuring timing and GPU metrics.

Usage:
    python run_microbench_maximus.py --maximus-dir /workspace/gpu_db --data-dir /workspace/gpu_db/tests --output-dir /workspace/gpu_db/results --n-reps 5
"""
import argparse
import csv
import os
import re
import subprocess
import threading
import time
from pathlib import Path


H2O_QUERIES = [
    "w1_001", "w1_002", "w1_003", "w1_004", "w1_005", "w1_006", "w1_007",
    "w2_008", "w2_009", "w2_010", "w2_011", "w2_012", "w2_013", "w2_014",
    "w3_016", "w3_017", "w3_018", "w3_019", "w3_020", "w3_021", "w3_023",
    "w4_027", "w4_028", "w4_029", "w4_030", "w4_031", "w4_032", "w4_033",
    "w6_015", "w6_022", "w6_024", "w6_025", "w6_026", "w6_034", "w6_035",
]

TPCH_QUERIES = [
    "w1_002", "w1_004", "w1_005", "w1_006", "w1_007", "w1_008", "w1_009",
    "w2_010", "w2_011", "w2_012", "w2_013", "w2_014", "w2_015", "w2_016", "w2_017",
    "w2_020", "w2_021",
    "w3_022", "w3_023", "w3_024", "w3_025", "w3_026", "w3_028", "w3_029",
    "w4_030", "w4_031", "w4_032", "w4_033", "w4_034", "w4_035", "w4_036",
    "w4_037", "w4_038", "w4_039", "w4_040", "w4_041", "w4_042",
    "w5a_043", "w5a_044", "w5a_045", "w5a_046", "w5a_047", "w5a_048",
    "w5b_049", "w5b_050", "w5b_051", "w5b_052", "w5b_053", "w5b_054",
    "w6_001", "w6_003", "w6_055", "w6_056", "w6_057",
]

CLICKBENCH_QUERIES = [
    "w1_001", "w1_002", "w1_003", "w1_004", "w1_005", "w1_006",
    "w2_007", "w2_008", "w2_009", "w2_010", "w2_018", "w2_019",
    "w3_011", "w3_012", "w3_013", "w3_014", "w3_015", "w3_027",
    "w4_021", "w4_022", "w4_023", "w4_024", "w4_025", "w4_058",
    "w6_016", "w6_017", "w6_018", "w6_019", "w6_020", "w6_022",
]

BENCHMARKS = {
    "microbench_h2o": {"queries": H2O_QUERIES, "data_subdir": "h2o", "scales": ["sf1"]},
    "microbench_tpch": {"queries": TPCH_QUERIES, "data_subdir": "tpch", "scales": ["sf1"]},
    "microbench_clickbench": {"queries": CLICKBENCH_QUERIES, "data_subdir": "clickbench", "scales": ["sf10"]},
}


class GPUSampler:
    def __init__(self, interval_ms=50):
        self.interval_s = interval_ms / 1000.0
        self.samples = []
        self._stop = threading.Event()
        self._thread = None
        self._meta = {}

    def start(self, **meta):
        self.samples = []
        self._stop.clear()
        self._meta = meta
        self._t0 = time.time()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
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
                    capture_output=True, text=True, timeout=2)
                if result.returncode == 0:
                    parts = result.stdout.strip().split(",")
                    if len(parts) >= 4:
                        self.samples.append({
                            **self._meta,
                            "time_offset_ms": round((time.time() - self._t0) * 1000, 2),
                            "power_w": float(parts[0].strip()),
                            "gpu_util_pct": float(parts[1].strip()),
                            "mem_used_mb": float(parts[2].strip()),
                            "pcie_gen": parts[3].strip(),
                        })
            except Exception:
                pass
            self._stop.wait(self.interval_s)


def run_maxbench(maxbench_path, benchmark, query, data_path, device, storage_device, n_reps):
    """Run a single maxbench query, return (times_ms_list, error_string)."""
    cmd = [
        str(maxbench_path),
        f"--benchmark={benchmark}",
        f"--queries={query}",
        f"--device={device}",
        f"--storage_device={storage_device}",
        "--engines=maximus",
        f"--n_reps={n_reps}",
        f"--path={data_path}",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        output = result.stdout + result.stderr
        if result.returncode != 0:
            # Extract error
            error = output.strip().split("\n")[-1] if output.strip() else f"exit code {result.returncode}"
            return None, error

        # Parse timing from output: look for "maximus" lines with timing
        times = []
        for line in output.split("\n"):
            # Pattern: "query_name    <time>ms" or CSV output
            m = re.search(r'(\d+)\s*ms', line)
            if m and "maximus" in line.lower():
                times.append(int(m.group(1)))

        # Alternative: parse CSV output if present
        if not times:
            for line in output.split("\n"):
                m = re.findall(r'(\d+)', line)
                if m and len(m) >= n_reps:
                    times = [int(x) for x in m[-n_reps:]]
                    break

        if not times:
            # Just check it ran successfully
            return [0], None  # ran but couldn't parse timing

        return times, None
    except subprocess.TimeoutExpired:
        return None, "TIMEOUT (300s)"
    except Exception as e:
        return None, str(e)


def main():
    parser = argparse.ArgumentParser(description="Run microbench via Maximus")
    parser.add_argument("--maximus-dir", type=str, default="/workspace/gpu_db")
    parser.add_argument("--data-dir", type=str, default="/workspace/gpu_db/tests")
    parser.add_argument("--output-dir", type=str, default="/workspace/gpu_db/results")
    parser.add_argument("--n-reps", type=int, default=5)
    parser.add_argument("--device", type=str, default="gpu")
    parser.add_argument("--storage-device", type=str, default="gpu")
    parser.add_argument("--sample-interval", type=int, default=50)
    parser.add_argument("--benchmarks", nargs="+",
                        default=["microbench_h2o", "microbench_tpch", "microbench_clickbench"])
    args = parser.parse_args()

    maxbench = Path(args.maximus_dir) / "build" / "benchmarks" / "maxbench"
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sampler = GPUSampler(interval_ms=args.sample_interval)

    timing_fields = ["engine", "benchmark", "scale", "query_id", "workload",
                     "min_ms", "avg_ms", "reps", "error"]
    sample_fields = ["engine", "benchmark", "scale", "query_id", "time_offset_ms",
                     "power_w", "gpu_util_pct", "mem_used_mb", "pcie_gen"]

    timing_path = output_dir / "microbench_maximus_timing.csv"
    samples_path = output_dir / "microbench_maximus_metrics.csv"

    # Remove old
    for p in [timing_path, samples_path]:
        if p.exists():
            p.unlink()

    ld_path = ":".join([
        "/root/arrow_install/lib",
        "/usr/local/lib/python3.10/dist-packages/nvidia/libnvcomp/lib64",
        "/usr/local/lib/python3.10/dist-packages/libkvikio/lib64",
        "/usr/local/lib/python3.10/dist-packages/libcudf/lib64",
        "/usr/local/lib/python3.10/dist-packages/librmm/lib64",
    ])
    os.environ["LD_LIBRARY_PATH"] = ld_path

    print(f"Maximus Microbench Runner")
    print(f"Device: {args.device}, Storage: {args.storage_device}, Reps: {args.n_reps}")
    print("=" * 60)

    total_ok = 0
    total_fail = 0

    for bench_name in args.benchmarks:
        if bench_name not in BENCHMARKS:
            print(f"Unknown benchmark: {bench_name}, skipping")
            continue

        bench = BENCHMARKS[bench_name]
        queries = bench["queries"]
        scale = bench["scales"][0]
        data_path = data_dir / bench["data_subdir"] / scale

        if not data_path.exists():
            print(f"\nData not found: {data_path}, skipping {bench_name}")
            continue

        print(f"\n### {bench_name.upper()} ({len(queries)} queries, scale={scale})")

        for qid in queries:
            workload = qid.split("_")[0]

            sampler.start(engine="maximus", benchmark=bench_name,
                         scale=scale, query_id=qid)

            times, error = run_maxbench(
                maxbench, bench_name, qid, str(data_path),
                args.device, args.storage_device, args.n_reps)

            samples = sampler.stop()

            if times and not error:
                min_ms = min(times) if times[0] > 0 else 0
                avg_ms = round(sum(times) / len(times), 1) if times[0] > 0 else 0
                reps_str = ",".join(str(t) for t in times)
                status = f"min={min_ms}ms avg={avg_ms}ms" if min_ms > 0 else "OK (timing unparsed)"
                print(f"  {qid}: {status}")
                total_ok += 1
            else:
                min_ms = avg_ms = 0
                reps_str = ""
                error_short = (error or "unknown")[:80]
                print(f"  {qid}: FAIL - {error_short}")
                total_fail += 1

            # Append timing
            write_header = not timing_path.exists()
            with open(timing_path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=timing_fields)
                if write_header:
                    w.writeheader()
                w.writerow({
                    "engine": "maximus", "benchmark": bench_name, "scale": scale,
                    "query_id": qid, "workload": workload,
                    "min_ms": min_ms, "avg_ms": avg_ms, "reps": reps_str,
                    "error": error or "",
                })

            # Append samples
            if samples:
                write_header = not samples_path.exists()
                with open(samples_path, "a", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=sample_fields)
                    if write_header:
                        w.writeheader()
                    w.writerows(samples)

    print(f"\n{'=' * 60}")
    print(f"Done. OK: {total_ok}, FAIL: {total_fail}")
    print(f"Results in {output_dir}")


if __name__ == "__main__":
    main()
