#!/usr/bin/env python3
"""
Sirius (DuckDB GPU extension) benchmark runner.

Usage:
    python run_sirius_benchmark.py [tpch] [h2o] [clickbench]
    python run_sirius_benchmark.py --sirius-dir /path/to/sirius --n-passes 3

Methodology:
  - 3 passes per (benchmark, SF), each pass in a SEPARATE DuckDB process
    to avoid GPU memory leaks that cause fallback after ~40-50 queries.
  - Within each pass, queries run in batches of 10 (configurable) to avoid OOM.
  - Each batch gets its own gpu_buffer_init call.
  - The 3rd (last) pass timing is recorded.
  - Auto-retry: if a query crashes within a batch, it's retried individually.

Output: CSV file with per-query timing and status (OK / FALLBACK / ERROR).
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
    detect_gpu, get_benchmark_config, sirius_db_path, sirius_query_dir,
    buffer_init_sql, MAXIMUS_DIR,
)

# ── Defaults ─────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SIRIUS_DIR = Path(os.environ.get("SIRIUS_DIR", str(MAXIMUS_DIR / "sirius")))
DEFAULT_RESULTS_DIR = MAXIMUS_DIR / "results"

# Set LD_LIBRARY_PATH for Sirius to find GPU libraries
import sysconfig as _sysconfig
_site = Path(_sysconfig.get_path("purelib"))
LD_EXTRA_SIRIUS = [
    str(p) for p in [
        _site / "nvidia" / "libnvcomp" / "lib64",
        _site / "libkvikio" / "lib64",
        _site / "libcudf" / "lib64",
        _site / "librmm" / "lib64",
        _site / "rapids_logger" / "lib64",
    ] if p.exists()
]
_ld = os.environ.get("LD_LIBRARY_PATH", "")
os.environ["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA_SIRIUS) + (":" + _ld if _ld else "")

# Detect GPU and build dynamic config
_gpu_info = detect_gpu()
BUFFER_INIT = buffer_init_sql(_gpu_info["vram_mb"])
N_PASSES = 3
BATCH_SIZE = 10       # queries per batch (avoids OOM)
QUERY_TIMEOUT_S = 60  # per-query timeout; >60s = FALLBACK

# Sirius-supported benchmarks only (no microbench)
_SIRIUS_BENCHMARKS = {"tpch", "h2o", "clickbench"}

RE_RUN_TIME = re.compile(r"Run Time \(s\):\s*real\s+([\d.]+)", re.IGNORECASE)
RE_MARKER = re.compile(r"===MARKER (\S+)===")


def load_queries(query_dir: Path):
    """Load SQL files containing gpu_processing() calls."""
    queries = []
    for sql_file in sorted(query_dir.glob("*.sql")):
        qname = sql_file.stem
        lines = sql_file.read_text().strip().splitlines()
        gpu_lines = [l.strip() for l in lines if l.strip().startswith("call gpu_processing(")]
        if gpu_lines:
            queries.append((qname, gpu_lines))
    return queries


def build_batch_sql(query_batch, buffer_init=BUFFER_INIT):
    """Build SQL: timer on, gpu_buffer_init, then queries with markers."""
    parts = [".timer on", buffer_init]
    for qname, gpu_lines in query_batch:
        parts.append(f".print ===MARKER {qname}===")
        parts.extend(gpu_lines)
    parts.append(".print ===END===")
    return "\n".join(parts) + "\n"


def parse_batch_output(stdout: str) -> dict:
    """Parse markers + Run Time to get {qname: (time_s, has_fallback)}."""
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


def run_single_pass(duckdb_bin: Path, db_path: Path, queries: list):
    """Run one pass: all queries in batches of BATCH_SIZE, each batch in own process."""
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
                [str(duckdb_bin), str(db_path)],
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
                        [str(duckdb_bin), str(db_path)],
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


def main():
    parser = argparse.ArgumentParser(description="Sirius GPU benchmark runner")
    parser.add_argument("benchmarks", nargs="*", default=["tpch", "h2o", "clickbench"])
    parser.add_argument("--sirius-dir", type=str, default=str(DEFAULT_SIRIUS_DIR))
    parser.add_argument("--results-dir", type=str, default=str(DEFAULT_RESULTS_DIR))
    parser.add_argument("--n-passes", type=int, default=N_PASSES)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--buffer-init", type=str, default=BUFFER_INIT)
    parser.add_argument("--test", action="store_true",
                        help="Test mode: use reduced query lists for quick validation")
    args = parser.parse_args()

    sirius_dir = Path(args.sirius_dir)
    duckdb_bin = sirius_dir / "build" / "release" / "duckdb"
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_bin.exists():
        print(f"ERROR: Sirius DuckDB binary not found: {duckdb_bin}")
        sys.exit(1)

    # Build dynamic benchmark config from hw_detect
    bench_config = get_benchmark_config(_gpu_info["vram_mb"], test_mode=args.test)
    BENCHMARKS = {k: v for k, v in bench_config.items() if k in _SIRIUS_BENCHMARKS}

    # Update module-level settings from args
    _batch_size = args.batch_size
    _buffer_init = args.buffer_init

    all_rows = []
    t0 = time.perf_counter()

    def _run_single_pass(db_path, queries):
        """Run one pass with configured batch size and buffer init."""
        all_data = {}
        batches = [queries[i:i+_batch_size] for i in range(0, len(queries), _batch_size)]
        for batch in batches:
            sql = build_batch_sql(batch, _buffer_init)
            with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
                f.write(sql)
                tmp = f.name
            total_timeout = QUERY_TIMEOUT_S * len(batch) + 120
            try:
                r = subprocess.run(
                    [str(duckdb_bin), str(db_path)],
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
            for qn, gl in batch:
                if qn not in all_data:
                    sql2 = build_batch_sql([(qn, gl)], _buffer_init)
                    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
                        f.write(sql2)
                        tmp2 = f.name
                    try:
                        r2 = subprocess.run(
                            [str(duckdb_bin), str(db_path)],
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

    for bench_name in args.benchmarks:
        if bench_name not in BENCHMARKS:
            print(f"Unknown benchmark: {bench_name}, skipping")
            continue
        cfg = BENCHMARKS[bench_name]
        queries = load_queries(sirius_query_dir(bench_name))
        # In test mode, filter to only the configured queries
        if args.test:
            allowed = set(cfg["queries"])
            queries = [(qn, gl) for qn, gl in queries if qn in allowed]

        for sf in cfg["scale_factors"]:
            db_path = sirius_db_path(bench_name, sf)
            if not db_path.exists():
                print(f"[SKIP] {bench_name} SF={sf}: {db_path} not found")
                continue

            print(f"\n{'='*60}")
            print(f"  {bench_name.upper()} SF={sf} ({len(queries)} queries)")
            print(f"{'='*60}")
            sys.stdout.flush()

            # Run N passes
            all_pass_data = []
            for p in range(args.n_passes):
                print(f"  Pass {p+1}/{args.n_passes}...")
                sys.stdout.flush()
                all_pass_data.append(_run_single_pass(db_path, queries))

            # Record last pass timing
            ok = 0
            for qname, _ in queries:
                last = all_pass_data[-1] if all_pass_data else {}
                t, fb = last.get(qname, (-1, False))

                if fb or t > QUERY_TIMEOUT_S:
                    status = "FALLBACK"
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

            print(f"  --- {ok}/{len(queries)} OK")
            sys.stdout.flush()

    elapsed = time.perf_counter() - t0
    csv_path = results_dir / "sirius_benchmark.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["benchmark", "sf", "query",
                                           "wall_time_s", "status", "times_all_passes"])
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
        fb_t = sum(1 for r in rows if r["status"] == "FALLBACK")
        err_t = sum(1 for r in rows if r["status"] == "ERROR")
        print(f"  {bench.upper()}: {ok_t}/{len(rows)} OK, {fb_t} FALLBACK, {err_t} ERROR")
        for sf in BENCHMARKS[bench]["scale_factors"]:
            sf_rows = [r for r in rows if str(r["sf"]) == str(sf)]
            if sf_rows:
                ok_n = sum(1 for r in sf_rows if r["status"] == "OK")
                fail_q = [r["query"] for r in sf_rows if r["status"] != "OK"]
                line = f"    SF={sf}: {ok_n}/{len(sf_rows)} OK"
                if fail_q:
                    line += f"  [{', '.join(fail_q)}]"
                print(line)


if __name__ == "__main__":
    main()
