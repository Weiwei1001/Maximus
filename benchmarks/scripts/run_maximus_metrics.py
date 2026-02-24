#!/usr/bin/env python3
"""
Maximus GPU steady-state metrics measurement.

Measures GPU power consumption and energy for each query under sustained load.
The methodology:
  1. Data loaded to GPU (-s gpu) for realistic query execution
  2. Calibration: 3 reps per query to measure base latency
  3. n_reps calculated so that n_reps * query_latency >= TARGET_TIME_S (default 10s)
  4. nvidia-smi sampled at 50ms intervals during sustained execution
  5. First/last 10% of samples trimmed for steady-state analysis

This gives accurate steady-state power and energy readings because the GPU
is under sustained compute load for 10+ seconds per query.

Usage:
    python run_maximus_metrics.py [--sf 10] [--target-time 10] [--results-dir ./results]

Output:
    - *_metrics_summary.csv: per-query metrics (power, energy, timing, GPU util)
    - *_metrics_samples.csv: raw nvidia-smi samples at 50ms intervals
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import re
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

# ── Paths (adjust to your environment) ──────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
MAXIMUS_DIR = SCRIPT_DIR.parent.parent
MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"

LD_EXTRA = [
    "/usr/local/lib/python3.12/dist-packages/nvidia/libnvcomp/lib64",
    "/usr/local/lib/python3.12/dist-packages/libkvikio/lib64",
]

# ── Benchmark configurations ────────────────────────────────────────────────
BENCHMARKS = {
    "tpch": {
        "data_base": MAXIMUS_DIR / "tests" / "tpch",
        "data_pattern": "csv-{sf}",
        "scale_factors": [1, 2, 10, 20],
        "queries": [f"q{i}" for i in range(1, 23)],
    },
    "h2o": {
        "data_base": MAXIMUS_DIR / "tests" / "h2o",
        "data_pattern": "csv-{sf}",
        "scale_factors": ["1gb", "2gb", "3gb", "4gb"],
        "queries": [f"q{i}" for i in [1, 2, 3, 4, 5, 6, 7, 9, 10]],
    },
    "clickbench": {
        "data_base": MAXIMUS_DIR / "tests" / "clickbench",
        "data_pattern": "csv-{sf}",
        "scale_factors": [1, 2, 10, 20],
        # 39 working GPU queries (q18,q27,q28,q42 unsupported on cuDF GPU)
        "queries": [f"q{i}" for i in range(0, 43) if i not in (18, 27, 28, 42)],
    },
}

TARGET_TIME_S = 10   # target sustained execution time per query
MIN_REPS = 3         # minimum repetitions even for slow queries
CALIBRATION_REPS = 3
TIMEOUT = 300


def get_env():
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + ld if ld else "")
    return env


def run_maxbench(benchmark, query, n_reps, data_path, storage="gpu", timeout=TIMEOUT):
    """Run a single query with maxbench."""
    cmd = [
        str(MAXBENCH),
        "--benchmark", benchmark,
        "-q", query,
        "-d", "gpu",
        "-s", storage,
        "-r", str(n_reps),
        "--n_reps_storage", "1",
        "--path", str(data_path),
        "--engines", "maximus",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                timeout=timeout, env=get_env())
        return result.stdout + result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "TIMEOUT", -1
    except Exception as e:
        return f"ERROR: {e}", -2


def parse_timing(output, query):
    """Extract per-rep timing from maxbench summary line."""
    # Match: gpu,maximus,<query>,<times...>
    pattern = rf"gpu,maximus,{re.escape(query)},([\d,]+)"
    match = re.search(pattern, output)
    if match:
        times_str = match.group(1).rstrip(",")
        return [int(t) for t in times_str.split(",") if t.strip()]
    return []


def sample_gpu_metrics(stop_event, samples, interval=0.05):
    """Sample GPU metrics via nvidia-smi at 50ms intervals."""
    start = time.time()
    while not stop_event.is_set():
        try:
            r = subprocess.run(
                ["nvidia-smi",
                 "--query-gpu=power.draw,utilization.gpu,memory.used,pcie.link.gen.current",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5
            )
            if r.returncode == 0:
                line = r.stdout.strip()
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 3:
                    samples.append({
                        "time_offset_ms": int((time.time() - start) * 1000),
                        "power_w": float(parts[0]),
                        "gpu_util_pct": float(parts[1]),
                        "mem_used_mb": float(parts[2]),
                    })
        except Exception:
            pass
        stop_event.wait(interval)


def run_metrics_for_benchmark(benchmark, sf, data_path, queries, target_time_s,
                              results_dir):
    """Run full metrics measurement for one (benchmark, sf) combination."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = f"{benchmark}_sf{sf}"

    print(f"\n{'=' * 70}")
    print(f"  METRICS: {benchmark.upper()} SF={sf}")
    print(f"  Target: {target_time_s}s sustained execution per query")
    print(f"  Started: {datetime.now()}")
    print(f"{'=' * 70}")

    # ── Phase 1: Calibration ──────────────────────────────────────────────
    # Use -s cpu with n_reps_storage=1: data loaded to CPU once, first query rep
    # transfers to GPU, subsequent reps reuse GPU-cached data (pure GPU compute).
    # This avoids OOM from CSV parsing on GPU while giving the same steady-state
    # behavior as -s gpu for reps 2+.
    print(f"\n--- Phase 1: Calibration ({CALIBRATION_REPS} reps, -s cpu) ---")
    calibration = {}
    for q in queries:
        print(f"  {q}...", end=" ", flush=True)
        output, rc = run_maxbench(benchmark, q, CALIBRATION_REPS, data_path,
                                  storage="cpu")
        times = parse_timing(output, q) if rc >= 0 else []
        if times:
            calibration[q] = {"min_ms": min(times), "storage": "cpu"}
            print(f"{min(times)}ms")
        else:
            calibration[q] = {"min_ms": 0, "storage": "fail"}
            print("FAIL")

    # ── Phase 2: Calculate n_reps ─────────────────────────────────────────
    print(f"\n--- Phase 2: Calculate n_reps ---")
    for q in queries:
        cal = calibration[q]
        if cal["min_ms"] > 0:
            cal["n_reps"] = max(MIN_REPS,
                                math.ceil(target_time_s * 1000 / cal["min_ms"]))
        else:
            cal["n_reps"] = 100  # default for sub-1ms queries
        est_s = cal["n_reps"] * max(cal["min_ms"], 1) / 1000
        print(f"  {q}: {cal['min_ms']}ms x {cal['n_reps']} reps = {est_s:.1f}s "
              f"({cal['storage']})")

    # ── Phase 3: Metrics run with nvidia-smi sampling ─────────────────────
    print(f"\n--- Phase 3: Metrics run with nvidia-smi sampling ---")
    all_samples = []
    summaries = []

    for q in queries:
        cal = calibration[q]
        if cal["storage"] == "fail":
            print(f"  {q}: SKIP (calibration failed)")
            continue

        n_reps = cal["n_reps"]
        print(f"  {q} ({n_reps} reps, -s cpu)...", end=" ", flush=True)

        # Start GPU sampling
        samples = []
        stop_event = threading.Event()
        sampler = threading.Thread(target=sample_gpu_metrics,
                                   args=(stop_event, samples, 0.05))
        sampler.start()

        start_time = time.time()
        output, rc = run_maxbench(benchmark, q, n_reps, data_path,
                                  storage="cpu", timeout=600)
        elapsed = time.time() - start_time

        stop_event.set()
        sampler.join(timeout=5)

        # Parse timing
        times = parse_timing(output, q)
        status = "OK" if times else "FAIL"
        if rc == -1:
            status = "TIMEOUT"
        if "out_of_memory" in (output or "").lower():
            status = "OOM"

        # Tag samples
        run_id = f"{tag}_{q}"
        for s in samples:
            s["run_id"] = run_id
            s["sf"] = sf
            s["query"] = q
        all_samples.extend(samples)

        # Compute steady-state metrics (trim first/last 10%)
        if len(samples) > 10:
            trim = max(1, len(samples) // 10)
            steady = samples[trim:-trim]
        else:
            steady = samples

        if steady:
            avg_power = sum(s["power_w"] for s in steady) / len(steady)
            max_power = max(s["power_w"] for s in steady)
            max_mem = max(s["mem_used_mb"] for s in steady)
            avg_util = sum(s["gpu_util_pct"] for s in steady) / len(steady)
            max_util = max(s["gpu_util_pct"] for s in steady)
        else:
            avg_power = max_power = max_mem = avg_util = max_util = 0

        min_ms = min(times) if times else 0
        avg_ms = sum(times) / len(times) if times else 0
        energy_j = avg_power * elapsed  # Joules = Watts * seconds

        summaries.append({
            "run_id": run_id, "benchmark": benchmark, "sf": sf, "query": q,
            "storage": "cpu", "n_reps": n_reps,
            "min_ms": min_ms, "avg_ms": f"{avg_ms:.1f}",
            "elapsed_s": f"{elapsed:.2f}",
            "num_samples": len(samples),
            "num_steady_samples": len(steady),
            "avg_power_w": f"{avg_power:.1f}",
            "max_power_w": f"{max_power:.1f}",
            "max_mem_mb": f"{max_mem:.0f}",
            "avg_gpu_util": f"{avg_util:.1f}",
            "max_gpu_util": f"{max_util:.0f}",
            "energy_j": f"{energy_j:.1f}",
            "status": status,
        })

        print(f"{min_ms}ms, {elapsed:.1f}s, {avg_power:.0f}W, "
              f"{max_util:.0f}%util, {max_mem:.0f}MB, {energy_j:.0f}J [{status}]")

    # ── Save ──────────────────────────────────────────────────────────────
    samples_file = results_dir / f"maximus_{tag}_metrics_samples_{ts}.csv"
    with open(samples_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "run_id", "sf", "query", "time_offset_ms",
            "power_w", "gpu_util_pct", "mem_used_mb",
        ])
        w.writeheader()
        w.writerows(all_samples)

    summary_file = results_dir / f"maximus_{tag}_metrics_summary_{ts}.csv"
    with open(summary_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "run_id", "benchmark", "sf", "query", "storage", "n_reps",
            "min_ms", "avg_ms", "elapsed_s",
            "num_samples", "num_steady_samples",
            "avg_power_w", "max_power_w", "max_mem_mb",
            "avg_gpu_util", "max_gpu_util", "energy_j", "status",
        ])
        w.writeheader()
        w.writerows(summaries)

    print(f"\n  Samples: {samples_file} ({len(all_samples)} samples)")
    print(f"  Summary: {summary_file} ({len(summaries)} queries)")
    return summaries, all_samples


def main():
    parser = argparse.ArgumentParser(
        description="Maximus GPU steady-state metrics measurement")
    parser.add_argument("benchmarks", nargs="*", default=["clickbench"],
                        help="Benchmarks to measure (default: clickbench)")
    parser.add_argument("--sf", type=str, default=None,
                        help="Specific scale factor (default: all configured SFs)")
    parser.add_argument("--target-time", type=float, default=TARGET_TIME_S,
                        help=f"Target sustained time per query in seconds "
                             f"(default: {TARGET_TIME_S})")
    parser.add_argument("--results-dir", type=str, default=None,
                        help="Directory for output CSVs")
    args = parser.parse_args()

    results_dir = (Path(args.results_dir) if args.results_dir
                   else MAXIMUS_DIR / "benchmark_results")
    results_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("  MAXIMUS GPU STEADY-STATE METRICS")
    print(f"  Target: {args.target_time}s sustained execution per query")
    print(f"  Started: {datetime.now()}")
    print("=" * 70)

    for bench_name in args.benchmarks:
        if bench_name not in BENCHMARKS:
            print(f"Unknown benchmark: {bench_name}, skipping")
            continue
        cfg = BENCHMARKS[bench_name]

        sfs = cfg["scale_factors"]
        if args.sf is not None:
            # Filter to requested SF
            try:
                sf_val = int(args.sf)
            except ValueError:
                sf_val = args.sf
            sfs = [sf for sf in sfs if str(sf) == str(sf_val)]
            if not sfs:
                print(f"SF={args.sf} not configured for {bench_name}")
                continue

        for sf in sfs:
            data_path = cfg["data_base"] / cfg["data_pattern"].format(sf=sf)
            if not data_path.exists():
                print(f"[SKIP] {bench_name} SF={sf}: {data_path} not found")
                continue
            run_metrics_for_benchmark(
                bench_name, sf, data_path, cfg["queries"],
                args.target_time, results_dir)

    print(f"\n{'=' * 70}")
    print(f"  ALL DONE — {datetime.now()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
