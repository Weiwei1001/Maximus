#!/usr/bin/env python3
"""
Maximus GPU benchmark runner for TPC-H, H2O, and ClickBench.

Usage:
    python run_maximus_benchmark.py [tpch] [h2o] [clickbench]
    python run_maximus_benchmark.py --n-reps 5 --results-dir ./results tpch clickbench

Runs each benchmark on all configured scale factors. Data is loaded once to
GPU, then each query is executed N times (default 3). The minimum time across
repetitions is reported.

Output: CSV file per benchmark in the results directory.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# ── Paths (adjust to your environment) ──────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
MAXIMUS_DIR = SCRIPT_DIR.parent.parent          # benchmarks/scripts -> Maximus root
MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"

# Extra library paths needed by pip-installed cuDF
LD_EXTRA = [
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libkvikio/lib64",
]

# ── Benchmark configurations ────────────────────────────────────────────────
BENCHMARKS = {
    "tpch": {
        "data_base": MAXIMUS_DIR / "tests" / "tpch",
        "data_pattern": "csv-{sf}",
        "scale_factors": [1, 2, 5, 10],
        "queries": [f"q{i}" for i in range(1, 23)],
    },
    "h2o": {
        "data_base": MAXIMUS_DIR / "tests" / "h2o",
        "data_pattern": "csv-{sf}",
        "scale_factors": ["1gb", "2gb", "3gb", "4gb"],
        # q8 is not implemented in Maximus
        "queries": [f"q{i}" for i in [1, 2, 3, 4, 5, 6, 7, 9, 10]],
    },
    "clickbench": {
        "data_base": MAXIMUS_DIR / "tests" / "clickbench",
        "data_pattern": "csv-{sf}",
        "scale_factors": [5, 10, 20],
        # 39 working GPU queries (q18,q27,q28,q42 unsupported on cuDF GPU)
        "queries": [f"q{i}" for i in range(0, 43) if i not in (18, 27, 28, 42)],
    },
}


def get_env():
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + ld if ld else "")
    # Don't restrict CUDA_VISIBLE_DEVICES; maxbench auto-selects RTX 5080 (GPU 1)
    return env


def parse_maxbench_output(output: str) -> dict:
    """Parse maxbench stdout to extract per-query timing lists."""
    result: dict = {"load_times_ms": [], "query_times": {}}

    m = re.search(r"Loading times over repetitions \[ms\]:\s*(.*)", output)
    if m:
        ts = m.group(1).strip().rstrip(",")
        result["load_times_ms"] = [int(t.strip()) for t in ts.split(",") if t.strip()]

    current_query = None
    for line in output.split("\n"):
        qm = re.match(r"\s*QUERY (\w+)\s*", line.strip())
        if qm:
            current_query = qm.group(1)
        tm = re.match(r"- MAXIMUS TIMINGS \[ms\]:\s*(.*)", line.strip())
        if tm and current_query:
            ts = tm.group(1).strip().rstrip(",")
            times = [int(t.strip()) for t in ts.split(",") if t.strip()]
            result["query_times"][current_query] = times

    # Fallback: parse "gpu,maximus,qN,t1,t2,..." summary lines
    for line in output.split("\n"):
        if line.startswith("gpu,maximus,"):
            parts = line.strip().split(",")
            if len(parts) >= 4:
                qname = parts[2]
                times = [int(t) for t in parts[3:] if t.strip()]
                if qname not in result["query_times"]:
                    result["query_times"][qname] = times
    return result


def run_maxbench(benchmark, data_path, queries, n_reps=3, timeout_s=300):
    cmd = [
        str(MAXBENCH),
        "--benchmark", benchmark,
        "-q", ",".join(queries),
        "-d", "gpu", "-r", str(n_reps),
        "--n_reps_storage", "1",
        "--path", str(data_path),
        "-s", "gpu", "--engines", "maximus",
    ]
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout_s, env=get_env())
        wall = time.perf_counter() - t0
        return proc.stdout + (proc.stderr or ""), wall, proc.returncode
    except subprocess.TimeoutExpired:
        return "", time.perf_counter() - t0, -1
    except Exception as e:
        return str(e), time.perf_counter() - t0, -1


def main():
    parser = argparse.ArgumentParser(description="Maximus GPU benchmark runner")
    parser.add_argument("benchmarks", nargs="*", default=["tpch", "h2o", "clickbench"])
    parser.add_argument("--n-reps", type=int, default=3, help="Number of timed repetitions")
    parser.add_argument("--results-dir", type=str, default=None,
                        help="Directory for output CSVs (default: <maximus>/benchmark_results)")
    args = parser.parse_args()

    results_dir = Path(args.results_dir) if args.results_dir else MAXIMUS_DIR / "benchmark_results"
    results_dir.mkdir(parents=True, exist_ok=True)
    n_reps = args.n_reps

    all_rows = []
    t0 = time.perf_counter()

    for bench_name in args.benchmarks:
        if bench_name not in BENCHMARKS:
            print(f"Unknown benchmark: {bench_name}, skipping")
            continue
        cfg = BENCHMARKS[bench_name]

        for sf in cfg["scale_factors"]:
            data_path = cfg["data_base"] / cfg["data_pattern"].format(sf=sf)
            if not data_path.exists():
                print(f"[SKIP] {bench_name} SF={sf}: {data_path} not found")
                continue

            queries = cfg["queries"]
            print(f"\n{'='*60}")
            print(f"  {bench_name.upper()} SF={sf} ({len(queries)} queries, {n_reps} reps)")
            print(f"{'='*60}")
            sys.stdout.flush()

            timeout = max(300, 120 * len(queries))
            output, wall, rc = run_maxbench(bench_name, data_path, queries,
                                            n_reps=n_reps, timeout_s=timeout)
            parsed = parse_maxbench_output(output)

            # Retry missing queries in smaller batches, then individually
            if rc != 0 or len(parsed["query_times"]) < len(queries) // 2:
                print(f"  Full batch incomplete ({len(parsed['query_times'])}/{len(queries)}), retrying...")
                for i in range(0, len(queries), 4):
                    batch = queries[i:i+4]
                    missing = [q for q in batch if q not in parsed["query_times"]]
                    if not missing:
                        continue
                    o2, _, _ = run_maxbench(bench_name, data_path, missing,
                                           n_reps=n_reps, timeout_s=120 * len(missing))
                    parsed["query_times"].update(parse_maxbench_output(o2)["query_times"])
                    for q in missing:
                        if q not in parsed["query_times"]:
                            o3, _, _ = run_maxbench(bench_name, data_path, [q],
                                                    n_reps=n_reps, timeout_s=120)
                            parsed["query_times"].update(
                                parse_maxbench_output(o3)["query_times"])

            ok = 0
            for q in queries:
                times = parsed["query_times"].get(q, [])
                if times:
                    status = "OK"
                    ok += 1
                    print(f"  {q}: min={min(times)}ms avg={sum(times)/len(times):.1f}ms [{status}]")
                else:
                    status = "FAIL"
                    print(f"  {q}: NO DATA [{status}]")
                all_rows.append({
                    "benchmark": bench_name, "sf": sf, "query": q,
                    "n_reps": len(times),
                    "min_ms": min(times) if times else "",
                    "avg_ms": round(sum(times)/len(times), 2) if times else "",
                    "max_ms": max(times) if times else "",
                    "status": status,
                })
            print(f"  --- {ok}/{len(queries)} OK")
            sys.stdout.flush()

    elapsed = time.perf_counter() - t0
    csv_path = results_dir / "maximus_benchmark.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["benchmark", "sf", "query", "n_reps",
                                           "min_ms", "avg_ms", "max_ms", "status"])
        w.writeheader()
        w.writerows(all_rows)

    print(f"\n{'='*60}")
    print(f"  DONE ({elapsed:.0f}s = {elapsed/60:.1f}min)")
    print(f"  Results: {csv_path}")
    print(f"{'='*60}")

    for bench in args.benchmarks:
        rows = [r for r in all_rows if r["benchmark"] == bench]
        if not rows:
            continue
        ok_t = sum(1 for r in rows if r["status"] == "OK")
        print(f"  {bench.upper()}: {ok_t}/{len(rows)} OK")
        for sf in BENCHMARKS[bench]["scale_factors"]:
            sf_rows = [r for r in rows if str(r["sf"]) == str(sf)]
            if sf_rows:
                ok_n = sum(1 for r in sf_rows if r["status"] == "OK")
                fail_q = [r["query"] for r in sf_rows if r["status"] != "OK"]
                line = f"    SF={sf}: {ok_n}/{len(sf_rows)} OK"
                if fail_q:
                    line += f"  FAIL: {', '.join(fail_q)}"
                print(line)


if __name__ == "__main__":
    main()
