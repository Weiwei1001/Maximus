#!/usr/bin/env python3
"""
Microbench timing runner for both Maximus and Sirius engines.

Usage:
    python3 run_microbench_timing.py                            # all microbench, default SFs
    python3 run_microbench_timing.py --generate-data            # generate missing data first
    python3 run_microbench_timing.py --engines maximus          # Maximus only
    python3 run_microbench_timing.py --benchmarks microbench_tpch  # TPC-H only
    python3 run_microbench_timing.py --scale-factors 10,20      # custom SFs

Default scale factors (larger than standard bench to avoid 0ms queries):
    microbench_tpch:       10, 20
    microbench_h2o:        4gb, 8gb
    microbench_clickbench: 10, 20
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

from hw_detect import (
    detect_gpu, get_benchmark_config, maximus_data_dir,
    sirius_db_path, sirius_query_dir, buffer_init_sql, MAXIMUS_DIR,
)

# ── Library paths for GPU binaries ───────────────────────────────────────────
import site as _site_mod
import sysconfig as _sysconfig

_site_dirs = list(_site_mod.getsitepackages()) + [_sysconfig.get_path("purelib")]
for _venv in ["/venv/main", os.environ.get("VIRTUAL_ENV", "")]:
    if _venv:
        import glob as _glob
        _site_dirs.extend(_glob.glob(f"{_venv}/lib/python*/site-packages"))
_LIB_SUBDIRS = [
    "nvidia/libnvcomp/lib64",
    "libkvikio/lib64",
    "libcudf/lib64",
    "librmm/lib64",
    "rapids_logger/lib64",
]
LD_EXTRA = []
for sd in _site_dirs:
    for sub in _LIB_SUBDIRS:
        p = Path(sd) / sub
        if p.exists() and str(p) not in LD_EXTRA:
            LD_EXTRA.append(str(p))

MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"
DUCKDB_BIN = MAXIMUS_DIR / "sirius" / "build" / "release" / "duckdb"
DATA_DIR = MAXIMUS_DIR / "benchmarks" / "data"

# ── GPU info ─────────────────────────────────────────────────────────────────
_gpu_info = detect_gpu()
BENCHMARKS = get_benchmark_config(_gpu_info["vram_mb"])
BUFFER_INIT = buffer_init_sql(_gpu_info["vram_mb"])

BATCH_SIZE = 10
QUERY_TIMEOUT_S = 60
RE_RUN_TIME = re.compile(r"Run Time \(s\):\s*real\s+([\d.]+)", re.IGNORECASE)
RE_MARKER = re.compile(r"===MARKER (\S+)===")

# Default: larger SFs to avoid 0ms queries
DEFAULT_SFS = {
    "microbench_tpch": [10],
    "microbench_h2o": ["4gb"],
    "microbench_clickbench": [10],
}


def get_env():
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + ld if ld else "")
    return env


# ═══════════════════════════════════════════════════════════════════════════════
#  Data Generation
# ═══════════════════════════════════════════════════════════════════════════════

def generate_missing_data(bench_names: list, sfs_by_bench: dict, engines: list):
    """Generate missing CSV (Maximus) and DuckDB (Sirius) data files."""
    print("\n" + "=" * 60)
    print("  Checking / generating missing data")
    print("=" * 60)

    for bench_name in bench_names:
        base = bench_name.replace("microbench_", "")  # tpch, h2o, clickbench
        sfs = sfs_by_bench[bench_name]

        for sf in sfs:
            # Check Maximus CSV
            if "maximus" in engines:
                csv_path = maximus_data_dir(bench_name, sf)
                if not csv_path.exists():
                    print(f"\n  [GEN] Maximus CSV: {bench_name} SF={sf}")
                    _generate_csv(base, sf)

            # Check Sirius DuckDB
            if "sirius" in engines:
                db_path = sirius_db_path(bench_name, sf)
                if not db_path.exists():
                    print(f"\n  [GEN] Sirius DuckDB: {bench_name} SF={sf}")
                    _generate_duckdb(base, sf)

    print("\n  Data check complete.\n")


def _generate_tpch_csv_from_duckdb(sf):
    """Export TPC-H CSV from DuckDB database."""
    import duckdb
    db_path = MAXIMUS_DIR / "tests" / "tpch_duckdb" / f"tpch_sf{sf}.duckdb"
    csv_dir = MAXIMUS_DIR / "tests" / "tpch" / f"csv-{sf}"
    csv_dir.mkdir(parents=True, exist_ok=True)
    print(f"    Exporting TPC-H SF={sf} from DuckDB to CSV...")
    con = duckdb.connect(str(db_path), read_only=True)
    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()]
    for t in tables:
        con.execute(f"COPY {t} TO '{csv_dir / (t + '.csv')}' (HEADER, DELIMITER ',')")
        print(f"      Exported {t}.csv")
    con.close()


def _generate_csv(base: str, sf):
    """Generate CSV data for a given benchmark base and scale factor."""
    if base == "tpch":
        # First ensure DuckDB exists, then export CSV
        db_path = MAXIMUS_DIR / "tests" / "tpch_duckdb" / f"tpch_sf{sf}.duckdb"
        if not db_path.exists():
            _generate_duckdb(base, sf)
        _generate_tpch_csv_from_duckdb(sf)
    elif base == "h2o":
        h2o_dir = MAXIMUS_DIR / "tests" / "h2o"
        h2o_dir.mkdir(parents=True, exist_ok=True)
        cmd = [sys.executable, str(DATA_DIR / "generate_h2o.py"),
               "-o", str(h2o_dir), "--format", "csv", str(sf)]
        subprocess.run(cmd, check=True)
    elif base == "clickbench":
        cb_dir = MAXIMUS_DIR / "tests" / "clickbench"
        parquet = cb_dir / "clickbench.parquet"
        cmd = [sys.executable, str(DATA_DIR / "generate_clickbench.py"),
               "-o", str(cb_dir), "--format", "csv", "--scales", str(sf)]
        if parquet.exists():
            cmd.extend(["--parquet-path", str(parquet)])
        subprocess.run(cmd, check=True)


def _generate_duckdb(base: str, sf):
    """Generate DuckDB data for a given benchmark base and scale factor."""
    if base == "tpch":
        db_dir = MAXIMUS_DIR / "tests" / "tpch_duckdb"
        db_dir.mkdir(parents=True, exist_ok=True)
        cmd = [sys.executable, str(DATA_DIR / "generate_tpch.py"),
               "-o", str(db_dir), "--scale-factors", str(sf),
               "--no-run-query", "--skip-install"]
        subprocess.run(cmd, check=True)
    elif base == "h2o":
        db_dir = MAXIMUS_DIR / "tests" / "h2o_duckdb"
        db_dir.mkdir(parents=True, exist_ok=True)
        cmd = [sys.executable, str(DATA_DIR / "generate_h2o.py"),
               "-o", str(db_dir), "--format", "duckdb", str(sf)]
        subprocess.run(cmd, check=True)
    elif base == "clickbench":
        db_dir = MAXIMUS_DIR / "tests" / "click_duckdb"
        db_dir.mkdir(parents=True, exist_ok=True)
        parquet = MAXIMUS_DIR / "tests" / "clickbench" / "clickbench.parquet"
        cmd = [sys.executable, str(DATA_DIR / "generate_clickbench.py"),
               "-o", str(db_dir), "--format", "duckdb", "--scales", str(sf)]
        if parquet.exists():
            cmd.extend(["--parquet-path", str(parquet)])
        subprocess.run(cmd, check=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  Maximus
# ═══════════════════════════════════════════════════════════════════════════════

def parse_maxbench_output(output: str) -> dict:
    result: dict = {"query_times": {}}
    current_query = None
    for line in output.split("\n"):
        qm = re.match(r"\s*QUERY (\w+)\s*", line.strip())
        if qm:
            current_query = qm.group(1)
        tm = re.match(r"- MAXIMUS TIMINGS \[ms\]:\s*(.*)", line.strip())
        if tm and current_query:
            ts = tm.group(1).strip().rstrip(",")
            times = [float(t.strip()) for t in ts.split(",") if t.strip()]
            result["query_times"][current_query] = times
    # Fallback: parse summary lines
    for line in output.split("\n"):
        if line.startswith("gpu,maximus,"):
            parts = line.strip().split(",")
            if len(parts) >= 4:
                qname = parts[2]
                times = [float(t) for t in parts[3:] if t.strip()]
                if qname not in result["query_times"]:
                    result["query_times"][qname] = times
    return result


def run_maximus(bench_name, sf, queries, n_reps):
    data_path = maximus_data_dir(bench_name, sf)
    if not data_path.exists():
        print(f"  [SKIP] {bench_name} SF={sf}: {data_path} not found")
        return []

    print(f"\n{'='*60}")
    print(f"  MAXIMUS | {bench_name.upper()} SF={sf} ({len(queries)} queries, {n_reps} reps)")
    print(f"{'='*60}")
    sys.stdout.flush()

    cmd = [
        str(MAXBENCH),
        "--benchmark", bench_name,
        "-q", ",".join(queries),
        "-d", "gpu", "-r", str(n_reps),
        "--n_reps_storage", "1",
        "--path", str(data_path),
        "-s", "gpu", "--engines", "maximus",
    ]
    timeout = max(300, 120 * len(queries))
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout, env=get_env())
        output = proc.stdout + (proc.stderr or "")
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        output, rc = "", -1
    except Exception as e:
        output, rc = str(e), -1
    wall = time.perf_counter() - t0

    parsed = parse_maxbench_output(output)

    # If batch failed, retry queries individually
    if rc != 0 or len(parsed["query_times"]) < len(queries) // 2:
        print(f"  Batch incomplete ({len(parsed['query_times'])}/{len(queries)}), retrying individually...")
        for q in queries:
            if q in parsed["query_times"]:
                continue
            cmd_single = [
                str(MAXBENCH),
                "--benchmark", bench_name,
                "-q", q,
                "-d", "gpu", "-r", str(n_reps),
                "--n_reps_storage", "1",
                "--path", str(data_path),
                "-s", "gpu", "--engines", "maximus",
            ]
            try:
                proc = subprocess.run(cmd_single, capture_output=True, text=True,
                                      timeout=120, env=get_env())
                p2 = parse_maxbench_output(proc.stdout + (proc.stderr or ""))
                parsed["query_times"].update(p2["query_times"])
            except Exception:
                pass

    rows = []
    ok = 0
    for q in queries:
        times = parsed["query_times"].get(q, [])
        if times:
            min_t = min(times)
            status = "OK"
            ok += 1
            print(f"  {q}: {min_t:.3f}ms [OK]")
        else:
            min_t = -1
            status = "FAIL"
            print(f"  {q}: NO DATA [FAIL]")
        rows.append({
            "engine": "maximus", "benchmark": bench_name, "sf": sf,
            "query": q, "min_time_ms": round(min_t, 4), "status": status,
            "all_times_ms": str(times),
        })
    print(f"  --- {ok}/{len(queries)} OK  (wall {wall:.1f}s)")
    sys.stdout.flush()
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
#  Sirius
# ═══════════════════════════════════════════════════════════════════════════════

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


def parse_sirius_output(stdout: str) -> dict:
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


def run_sirius_pass(db_path, queries):
    all_data = {}
    batches = [queries[i:i+BATCH_SIZE] for i in range(0, len(queries), BATCH_SIZE)]
    for batch in batches:
        sql = build_batch_sql(batch)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(sql)
            tmp = f.name
        total_timeout = QUERY_TIMEOUT_S * len(batch) + 120
        try:
            r = subprocess.run(
                [str(DUCKDB_BIN), str(db_path)],
                stdin=open(tmp, "r"),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=total_timeout,
            )
            all_data.update(parse_sirius_output(r.stdout or ""))
        except Exception:
            pass
        finally:
            os.unlink(tmp)
        # Retry missing individually
        for qn, gl in batch:
            if qn not in all_data:
                sql2 = build_batch_sql([(qn, gl)])
                with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
                    f.write(sql2)
                    tmp2 = f.name
                try:
                    r2 = subprocess.run(
                        [str(DUCKDB_BIN), str(db_path)],
                        stdin=open(tmp2, "r"),
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        text=True, timeout=QUERY_TIMEOUT_S + 60,
                    )
                    all_data.update(parse_sirius_output(r2.stdout or ""))
                except Exception:
                    all_data[qn] = (-1, False)
                finally:
                    os.unlink(tmp2)
    return all_data


def run_sirius(bench_name, sf, n_passes):
    db_path = sirius_db_path(bench_name, sf)
    if not db_path.exists():
        print(f"  [SKIP] {bench_name} SF={sf}: {db_path} not found")
        return []

    query_dir = sirius_query_dir(bench_name)
    queries = load_sirius_queries(query_dir)
    if not queries:
        print(f"  [SKIP] {bench_name}: no queries found in {query_dir}")
        return []

    # Filter to configured query list
    cfg_queries = set(BENCHMARKS[bench_name]["queries"])
    queries = [(qn, gl) for qn, gl in queries if qn in cfg_queries]

    print(f"\n{'='*60}")
    print(f"  SIRIUS | {bench_name.upper()} SF={sf} ({len(queries)} queries, {n_passes} passes)")
    print(f"{'='*60}")
    sys.stdout.flush()

    t0 = time.perf_counter()
    all_pass_data = []
    for p in range(n_passes):
        print(f"  Pass {p+1}/{n_passes}...")
        sys.stdout.flush()
        all_pass_data.append(run_sirius_pass(db_path, queries))

    rows = []
    ok = 0
    last = all_pass_data[-1] if all_pass_data else {}
    for qname, _ in queries:
        t, fb = last.get(qname, (-1, False))
        if fb or t > QUERY_TIMEOUT_S:
            status = "FALLBACK"
        elif t < 0:
            status = "ERROR"
        else:
            status = "OK"
            ok += 1
        time_str = f"{t*1000:.3f}ms" if t >= 0 else "ERR"
        print(f"  {qname}: {time_str} [{status}]")
        rows.append({
            "engine": "sirius", "benchmark": bench_name, "sf": sf,
            "query": qname, "min_time_ms": round(t * 1000, 4) if t >= 0 else -1,
            "status": status,
            "all_times_ms": str([pd.get(qname, (-1, False))[0] for pd in all_pass_data]),
        })
    wall = time.perf_counter() - t0
    print(f"  --- {ok}/{len(queries)} OK  (wall {wall:.1f}s)")
    sys.stdout.flush()
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════

def parse_sf(s):
    try:
        return int(s)
    except ValueError:
        return s


def main():
    parser = argparse.ArgumentParser(description="Microbench timing for Maximus & Sirius")
    parser.add_argument("--engines", type=str, default="maximus,sirius",
                        help="Comma-separated engines (default: maximus,sirius)")
    parser.add_argument("--benchmarks", type=str,
                        default="microbench_tpch,microbench_h2o,microbench_clickbench",
                        help="Comma-separated benchmark names")
    parser.add_argument("--scale-factors", type=str, default=None,
                        help="Comma-separated SFs (default per bench: tpch=10,20 h2o=4gb,8gb click=10,20)")
    parser.add_argument("--n-reps", type=int, default=3,
                        help="Maximus repetitions (default: 3)")
    parser.add_argument("--n-passes", type=int, default=3,
                        help="Sirius passes (default: 3)")
    parser.add_argument("--results-dir", type=str,
                        default=str(MAXIMUS_DIR / "results"),
                        help="Output directory for CSV")
    parser.add_argument("--generate-data", action="store_true",
                        help="Generate missing CSV/DuckDB data before benchmarking")
    args = parser.parse_args()

    engines = [e.strip() for e in args.engines.split(",")]
    bench_names = [b.strip() for b in args.benchmarks.split(",")]
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    # Resolve scale factors per benchmark
    sfs_by_bench = {}
    for bench_name in bench_names:
        if bench_name not in BENCHMARKS:
            continue
        if args.scale_factors:
            sfs_by_bench[bench_name] = [parse_sf(s) for s in args.scale_factors.split(",")]
        else:
            sfs_by_bench[bench_name] = DEFAULT_SFS.get(bench_name,
                                                        BENCHMARKS[bench_name]["scale_factors"][:2])

    # Generate missing data if requested
    if args.generate_data:
        generate_missing_data(bench_names, sfs_by_bench, engines)

    all_rows = []
    t0 = time.perf_counter()

    for bench_name in bench_names:
        if bench_name not in BENCHMARKS:
            print(f"Unknown benchmark: {bench_name}, skipping")
            continue

        cfg = BENCHMARKS[bench_name]
        sfs = sfs_by_bench[bench_name]

        for sf in sfs:
            if "maximus" in engines:
                rows = run_maximus(bench_name, sf, cfg["queries"], args.n_reps)
                all_rows.extend(rows)

            if "sirius" in engines:
                rows = run_sirius(bench_name, sf, args.n_passes)
                all_rows.extend(rows)

    total_wall = time.perf_counter() - t0

    # Write CSV
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_csv = results_dir / f"microbench_timing_{ts}.csv"
    if all_rows:
        fieldnames = ["engine", "benchmark", "sf", "query", "min_time_ms",
                      "status", "all_times_ms"]
        with open(out_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(all_rows)
        print(f"\nResults saved to {out_csv}")

    # Summary
    ok_count = sum(1 for r in all_rows if r["status"] == "OK")
    print(f"\nDone: {ok_count}/{len(all_rows)} OK, total wall time {total_wall:.0f}s")


if __name__ == "__main__":
    main()
