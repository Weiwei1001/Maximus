#!/usr/bin/env python3
"""
Run both Maximus and Sirius benchmarks sequentially, then compare.
Careful with GPU memory - runs one engine at a time.

Usage:
    python run_all.py                       # Run both engines, all benchmarks
    python run_all.py --engine maximus      # Maximus only
    python run_all.py --engine sirius       # Sirius only
    python run_all.py --benchmarks tpch h2o # Specific benchmarks only
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# ── Paths (relative to this script) ──────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
MAXIMUS_DIR = SCRIPT_DIR.parent.parent  # benchmarks/scripts -> Maximus root
MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"
SIRIUS_BIN = Path(os.environ.get("SIRIUS_BIN", MAXIMUS_DIR / "sirius" / "build" / "release" / "duckdb"))
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", MAXIMUS_DIR / "results"))
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

LD_EXTRA = [
    "/usr/local/lib/python3.12/dist-packages/nvidia/libnvcomp/lib64",
    "/usr/local/lib/python3.12/dist-packages/libkvikio/lib64",
]

# ── Sirius settings ──────────────────────────────────────────────────────────
BUFFER_INIT = 'call gpu_buffer_init("20 GB", "10 GB");'
BATCH_SIZE = 10
QUERY_TIMEOUT_S = 60
N_PASSES = 3

RE_RUN_TIME = re.compile(r"Run Time \(s\):\s*real\s+([\d.]+)", re.IGNORECASE)
RE_MARKER = re.compile(r"===MARKER (\S+)===")

# ── Data directories ─────────────────────────────────────────────────────────
# Maximus test data lives under MAXIMUS_DIR/tests/{benchmark}/csv-{sf}
# Sirius DuckDB databases live in a configurable directory (default: sibling of Maximus)
SIRIUS_DATA_DIR = Path(os.environ.get("SIRIUS_DATA_DIR", MAXIMUS_DIR.parent))

# ── Benchmark configs ─────────────────────────────────────────────────────────
MAXIMUS_BENCHMARKS = {
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
        "scale_factors": [1, 2],
        "queries": [f"q{i}" for i in range(0, 43) if i not in (18, 27, 28, 42)],
    },
}

SIRIUS_BENCHMARKS = {
    "tpch": {
        "db_dir": SIRIUS_DATA_DIR / "tpch_duckdb",
        "db_pattern": "tpch_sf{sf}.duckdb",
        "query_dir": SIRIUS_DATA_DIR / "tpch_sql" / "queries" / "1",
        "scale_factors": [1, 2, 10, 20],
    },
    "h2o": {
        "db_dir": SIRIUS_DATA_DIR / "h2o_duckdb",
        "db_pattern": "h2o_{sf}.duckdb",
        "query_dir": SIRIUS_DATA_DIR / "h2o_sql" / "queries" / "1",
        "scale_factors": ["1gb", "2gb", "3gb", "4gb"],
    },
    "clickbench": {
        "db_dir": SIRIUS_DATA_DIR / "click_duckdb",
        "db_pattern": "clickbench_{sf}.duckdb",
        "query_dir": SIRIUS_DATA_DIR / "click_sql" / "queries" / "1",
        "scale_factors": [1, 2],
    },
}


# ══════════════════════════════════════════════════════════════════════════════
#  Maximus Runner
# ══════════════════════════════════════════════════════════════════════════════

def get_env():
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + ld if ld else "")
    return env


def parse_maxbench_output(output: str) -> dict:
    result = {"load_times_ms": [], "query_times": {}}
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


def run_maximus_benchmarks(selected_benchmarks=None):
    print("\n" + "=" * 70)
    print("  MAXIMUS BENCHMARKS")
    print("=" * 70)

    all_rows = []
    t0 = time.perf_counter()

    for bench_name, cfg in MAXIMUS_BENCHMARKS.items():
        if selected_benchmarks and bench_name not in selected_benchmarks:
            continue
        for sf in cfg["scale_factors"]:
            data_path = cfg["data_base"] / cfg["data_pattern"].format(sf=sf)
            if not data_path.exists():
                print(f"[SKIP] {bench_name} SF={sf}: {data_path} not found")
                continue

            queries = cfg["queries"]
            print(f"\n{'=' * 60}")
            print(f"  MAXIMUS {bench_name.upper()} SF={sf} ({len(queries)} queries, 3 reps)")
            print(f"{'=' * 60}")
            sys.stdout.flush()

            timeout = max(300, 120 * len(queries))
            output, wall, rc = run_maxbench(bench_name, data_path, queries,
                                            n_reps=3, timeout_s=timeout)
            parsed = parse_maxbench_output(output)

            # Retry missing queries
            if rc != 0 or len(parsed["query_times"]) < len(queries) // 2:
                print(f"  Full batch incomplete ({len(parsed['query_times'])}/{len(queries)}), retrying...")
                for i in range(0, len(queries), 4):
                    batch = queries[i:i + 4]
                    missing = [q for q in batch if q not in parsed["query_times"]]
                    if not missing:
                        continue
                    o2, _, _ = run_maxbench(bench_name, data_path, missing,
                                           n_reps=3, timeout_s=120 * len(missing))
                    parsed["query_times"].update(parse_maxbench_output(o2)["query_times"])
                    for q in missing:
                        if q not in parsed["query_times"]:
                            o3, _, _ = run_maxbench(bench_name, data_path, [q],
                                                    n_reps=3, timeout_s=120)
                            parsed["query_times"].update(
                                parse_maxbench_output(o3)["query_times"])

            ok = 0
            for q in queries:
                times = parsed["query_times"].get(q, [])
                if times:
                    status = "OK"
                    ok += 1
                    print(f"  {q}: min={min(times)}ms avg={sum(times) / len(times):.1f}ms [{status}]")
                else:
                    status = "FAIL"
                    print(f"  {q}: NO DATA [{status}]")
                all_rows.append({
                    "benchmark": bench_name, "sf": sf, "query": q,
                    "n_reps": len(times),
                    "min_ms": min(times) if times else "",
                    "avg_ms": round(sum(times) / len(times), 2) if times else "",
                    "max_ms": max(times) if times else "",
                    "status": status,
                })
            print(f"  --- {ok}/{len(queries)} OK ({time.perf_counter() - t0:.0f}s elapsed)")
            sys.stdout.flush()

    elapsed = time.perf_counter() - t0
    csv_path = RESULTS_DIR / "maximus_benchmark.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["benchmark", "sf", "query", "n_reps",
                                           "min_ms", "avg_ms", "max_ms", "status"])
        w.writeheader()
        w.writerows(all_rows)

    print(f"\n  MAXIMUS DONE ({elapsed:.0f}s = {elapsed / 60:.1f}min)")
    print(f"  Results: {csv_path}")

    for bench in MAXIMUS_BENCHMARKS:
        rows = [r for r in all_rows if r["benchmark"] == bench]
        if not rows:
            continue
        ok_t = sum(1 for r in rows if r["status"] == "OK")
        print(f"  {bench.upper()}: {ok_t}/{len(rows)} OK")

    return all_rows


# ══════════════════════════════════════════════════════════════════════════════
#  Sirius Runner
# ══════════════════════════════════════════════════════════════════════════════

def load_sirius_queries(query_dir: Path):
    queries = []
    for sql_file in sorted(query_dir.glob("*.sql")):
        qname = sql_file.stem
        lines = sql_file.read_text().strip().splitlines()
        gpu_lines = [l.strip() for l in lines if l.strip().startswith("call gpu_processing(")]
        if gpu_lines:
            queries.append((qname, gpu_lines))
    return queries


def build_batch_sql(query_batch):
    parts = [".timer on", BUFFER_INIT]
    for qname, gpu_lines in query_batch:
        parts.append(f".print ===MARKER {qname}===")
        parts.extend(gpu_lines)
    parts.append(".print ===END===")
    return "\n".join(parts) + "\n"


def parse_batch_output(stdout: str) -> dict:
    markers = [(m.start(), m.group(1)) for m in RE_MARKER.finditer(stdout)]
    markers.append((len(stdout), "__END__"))
    query_data = {}
    for i in range(len(markers) - 1):
        pos, qname = markers[i]
        next_pos = markers[i + 1][0]
        section = stdout[pos:next_pos]
        times = [float(m.group(1)) for m in RE_RUN_TIME.finditer(section)]
        total = round(sum(times), 4) if times else -1
        has_fallback = "fallback" in section.lower()
        query_data[qname] = (total, has_fallback)
    return query_data


def run_sirius_single_pass(db_path: Path, queries: list):
    all_data = {}
    batches = [queries[i:i + BATCH_SIZE] for i in range(0, len(queries), BATCH_SIZE)]

    for batch in batches:
        sql = build_batch_sql(batch)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(sql)
            tmp = f.name
        total_timeout = QUERY_TIMEOUT_S * len(batch) + 120
        try:
            r = subprocess.run(
                [str(SIRIUS_BIN), str(db_path)],
                stdin=open(tmp, "r"),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=total_timeout,
            )
            batch_data = parse_batch_output(r.stdout or "")
            all_data.update(batch_data)
        except (subprocess.TimeoutExpired, Exception):
            pass
        finally:
            os.unlink(tmp)

        # Retry failed queries individually
        for qn, gl in batch:
            if qn not in all_data:
                sql2 = build_batch_sql([(qn, gl)])
                with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
                    f.write(sql2)
                    tmp2 = f.name
                try:
                    r2 = subprocess.run(
                        [str(SIRIUS_BIN), str(db_path)],
                        stdin=open(tmp2, "r"),
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        text=True, timeout=QUERY_TIMEOUT_S + 60,
                    )
                    all_data.update(parse_batch_output(r2.stdout or ""))
                except Exception:
                    all_data[qn] = (-1, False)
                finally:
                    os.unlink(tmp2)

    return all_data


def run_sirius_benchmarks(selected_benchmarks=None):
    print("\n" + "=" * 70)
    print("  SIRIUS BENCHMARKS")
    print("=" * 70)

    all_rows = []
    t0 = time.perf_counter()

    for bench_name, cfg in SIRIUS_BENCHMARKS.items():
        if selected_benchmarks and bench_name not in selected_benchmarks:
            continue
        queries = load_sirius_queries(cfg["query_dir"])

        for sf in cfg["scale_factors"]:
            db_path = cfg["db_dir"] / cfg["db_pattern"].format(sf=sf)
            if not db_path.exists():
                print(f"[SKIP] {bench_name} SF={sf}: {db_path} not found")
                continue

            print(f"\n{'=' * 60}")
            print(f"  SIRIUS {bench_name.upper()} SF={sf} ({len(queries)} queries, {N_PASSES} passes, batch={BATCH_SIZE})")
            print(f"{'=' * 60}")
            sys.stdout.flush()

            # Run N passes
            all_pass_data = []
            for p in range(N_PASSES):
                print(f"  Pass {p + 1}/{N_PASSES}...")
                sys.stdout.flush()
                all_pass_data.append(run_sirius_single_pass(db_path, queries))

            # Record last pass timing
            ok = 0
            fallback = 0
            for qname, _ in queries:
                last = all_pass_data[-1] if all_pass_data else {}
                t, fb = last.get(qname, (-1, False))

                if fb or t > QUERY_TIMEOUT_S:
                    status = "FALLBACK"
                    fallback += 1
                elif t < 0:
                    status = "ERROR"
                else:
                    status = "OK"
                    ok += 1

                pass_times = [pd.get(qname, (-1, False))[0] for pd in all_pass_data]
                time_str = f"{t:.3f}s" if t >= 0 else "ERR"
                print(f"  {qname}: {time_str} [{status}]  (passes: {pass_times})")

                all_rows.append({
                    "benchmark": bench_name, "sf": sf, "query": qname,
                    "wall_time_s": t, "status": status,
                    "times_all_passes": str(pass_times),
                })

            print(f"  --- {ok}/{len(queries)} OK, {fallback} FALLBACK ({time.perf_counter() - t0:.0f}s elapsed)")
            sys.stdout.flush()

    elapsed = time.perf_counter() - t0
    csv_path = RESULTS_DIR / "sirius_benchmark.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["benchmark", "sf", "query",
                                           "wall_time_s", "status", "times_all_passes"])
        w.writeheader()
        w.writerows(all_rows)

    print(f"\n  SIRIUS DONE ({elapsed:.0f}s = {elapsed / 60:.1f}min)")
    print(f"  Results: {csv_path}")

    for bench in SIRIUS_BENCHMARKS:
        rows = [r for r in all_rows if r["benchmark"] == bench]
        if not rows:
            continue
        ok_t = sum(1 for r in rows if r["status"] == "OK")
        fb_t = sum(1 for r in rows if r["status"] == "FALLBACK")
        print(f"  {bench.upper()}: {ok_t}/{len(rows)} OK, {fb_t} FALLBACK")

    return all_rows


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Run Maximus + Sirius GPU benchmarks")
    parser.add_argument("--engine", choices=["maximus", "sirius", "both"], default="both",
                        help="Which engine to benchmark (default: both)")
    parser.add_argument("--benchmarks", nargs="+", choices=["tpch", "h2o", "clickbench"],
                        help="Specific benchmarks to run (default: all)")
    parser.add_argument("--results-dir", type=str, help="Override results directory")
    args = parser.parse_args()

    if args.results_dir:
        global RESULTS_DIR
        RESULTS_DIR = Path(args.results_dir)
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("  FULL BENCHMARK RUN: Maximus + Sirius")
    print(f"  Engine: {args.engine}")
    print(f"  Methodology:")
    print(f"    Maximus: 3 reps, min time, data on GPU")
    print(f"    Sirius:  3 passes, 10 queries/batch, 3rd pass timing")
    print(f"  Running sequentially to avoid GPU contention")
    print("=" * 70)

    t_start = time.perf_counter()
    maximus_rows = []
    sirius_rows = []

    if args.engine in ("maximus", "both"):
        maximus_rows = run_maximus_benchmarks(args.benchmarks)

    if args.engine in ("sirius", "both"):
        sirius_rows = run_sirius_benchmarks(args.benchmarks)

    total = time.perf_counter() - t_start

    print("\n" + "=" * 70)
    print(f"  FINAL SUMMARY (total time: {total:.0f}s = {total / 60:.1f}min)")
    print("=" * 70)

    def summarize(name, rows, bench_configs):
        total_ok = sum(1 for r in rows if r["status"] == "OK")
        total_n = len(rows)
        print(f"\n  {name}: {total_ok}/{total_n} OK")
        for bench in bench_configs:
            br = [r for r in rows if r["benchmark"] == bench]
            if not br:
                continue
            ok_n = sum(1 for r in br if r["status"] == "OK")
            fb_n = sum(1 for r in br if r["status"] == "FALLBACK")
            fail_n = sum(1 for r in br if r["status"] not in ("OK", "FALLBACK"))
            line = f"    {bench.upper()}: {ok_n}/{len(br)} OK"
            if fb_n:
                line += f", {fb_n} FALLBACK"
            if fail_n:
                line += f", {fail_n} FAIL"
            print(line)

    if maximus_rows:
        summarize("MAXIMUS", maximus_rows, MAXIMUS_BENCHMARKS)
    if sirius_rows:
        summarize("SIRIUS", sirius_rows, SIRIUS_BENCHMARKS)


if __name__ == "__main__":
    main()
