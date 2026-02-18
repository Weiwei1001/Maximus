#!/usr/bin/env python3
"""
Run Maximus timing benchmarks for TPC-H, H2O, and ClickBench.

Runs maxbench with --storage_device=gpu for best performance.
Results are written to CSV in real-time (appended after each scale factor).

Usage:
    python run_timing.py --maximus-dir /path/to/Maximus --data-dir /path/to/data --n-reps 3
    python run_timing.py --maximus-dir /path/to/Maximus --data-dir /path/to/data --benchmarks tpch h2o
"""
import argparse
import csv
import os
import re
import subprocess
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


def build_env(maximus_dir: Path):
    """Build environment variables for maxbench."""
    env = os.environ.copy()
    build_dir = maximus_dir / "build"
    lib_paths = [
        "/arrow_install/lib",
        str(build_dir / "src" / "maximus"),
        str(build_dir / "third_party"),
    ]
    # Add conda/venv lib paths if they exist
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
    elapsed = time.time() - t0
    return result.stdout + result.stderr, elapsed


def extract_timings(output: str):
    """Extract per-query timings from maxbench output.

    Returns list of (query_name, [rep1_ms, rep2_ms, ...], min_ms, avg_ms).
    """
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
    """Append rows to a CSV file, creating it with headers if it doesn't exist."""
    write_header = not filepath.exists()
    with open(filepath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def run_benchmark(benchmark: str, config: dict, maximus_dir: Path, data_dir: Path,
                  output_dir: Path, n_reps: int, storage_device: str, env: dict):
    """Run a single benchmark across all scale factors."""
    csv_path = output_dir / f"{benchmark}_timing.csv"
    fieldnames = ["benchmark", "scale", "query", "min_ms", "avg_ms", "reps"]

    # Remove old results file to start fresh
    if csv_path.exists():
        csv_path.unlink()

    for sf in config["scales"]:
        data_path = str(data_dir / config["data_subdir"] / sf)
        if not Path(data_path).exists():
            print(f"  [{benchmark} {sf}] Data not found at {data_path}, skipping")
            continue

        print(f"\n  [{benchmark} {sf}] Running {config['queries'].count(',') + 1} queries x {n_reps} reps...")
        t0 = time.time()

        output, elapsed = run_maxbench(maximus_dir, benchmark, config["queries"],
                                       data_path, n_reps, storage_device, env)

        # Save raw output
        raw_path = output_dir / f"{benchmark}_raw_{sf}.txt"
        with open(raw_path, "w") as f:
            f.write(output)

        timings = extract_timings(output)
        if timings:
            rows = []
            for qname, reps, min_ms, avg_ms in timings:
                reps_str = ",".join(str(r) for r in reps)
                rows.append({
                    "benchmark": benchmark, "scale": sf, "query": qname,
                    "min_ms": min_ms, "avg_ms": avg_ms, "reps": reps_str,
                })
                print(f"    {qname}: min={min_ms}ms avg={avg_ms}ms [{reps_str}]")
            # Real-time CSV append
            append_csv(csv_path, rows, fieldnames)
            print(f"  [{benchmark} {sf}] Done in {elapsed:.1f}s, results appended to {csv_path}")
        else:
            print(f"  [{benchmark} {sf}] WARNING: No timings extracted! ({elapsed:.1f}s)")
            if "Error" in output or "terminate" in output:
                for line in output.split("\n"):
                    if any(w in line.lower() for w in ["error", "terminate", "fault"]):
                        print(f"    >>> {line.strip()}")


def main():
    parser = argparse.ArgumentParser(description="Run Maximus timing benchmarks")
    parser.add_argument("--maximus-dir", type=str, required=True, help="Path to Maximus repository")
    parser.add_argument("--data-dir", type=str, required=True,
                        help="Base data directory (contains tpch/, h2o/, clickbench/ subdirs)")
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory (default: data-dir)")
    parser.add_argument("--n-reps", type=int, default=3, help="Number of repetitions (default: 3)")
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
    print(f"  Maximus Timing Benchmark")
    print(f"  Start: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Storage device: {args.storage_device}")
    print(f"  Repetitions: {args.n_reps}")
    print(f"  Benchmarks: {args.benchmarks}")
    print("=" * 60)

    for bench in args.benchmarks:
        config = BENCHMARK_CONFIGS[bench]
        print(f"\n{'#' * 60}")
        print(f"#  {bench.upper()}")
        print(f"{'#' * 60}")
        run_benchmark(bench, config, maximus_dir, data_dir, output_dir, args.n_reps, args.storage_device, env)

    print(f"\n{'=' * 60}")
    print(f"  Timing complete: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Results in: {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
