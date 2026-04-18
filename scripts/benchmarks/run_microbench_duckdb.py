#!/usr/bin/env python3
"""Run microbench SQL queries via DuckDB (Sirius baseline).

Loads the benchmark data into DuckDB and runs each microbench SQL query,
measuring timing and optionally GPU metrics.

Usage:
    python run_microbench_duckdb.py --data-dir /path/to/tests --output-dir /path/to/results --n-reps 3
"""
import argparse
import csv
import os
import re
import subprocess
import threading
import time
from pathlib import Path

import duckdb


# Intersection of Maximus C++ plans and DuckDB/Sirius SQL files (79 queries)
INTERSECTION_H2O = {
    "w1_001", "w1_002", "w1_003", "w1_004", "w1_005", "w1_006", "w1_007",
    "w2_008", "w2_009", "w2_010", "w2_011", "w2_012", "w2_013", "w2_014",
    "w3_016", "w3_017", "w3_018", "w3_019", "w3_020", "w3_021", "w3_023",
    "w4_027", "w4_028", "w4_029", "w4_030", "w4_031", "w4_032", "w4_033",
    "w6_015", "w6_022", "w6_024", "w6_025", "w6_026", "w6_034", "w6_035",
}
INTERSECTION_TPCH = {
    "w1_002", "w1_004", "w1_005", "w1_006", "w1_007", "w1_008",
    "w2_012", "w2_013", "w2_014", "w2_015", "w2_016", "w2_017",
    "w3_023", "w3_024", "w3_025", "w3_028",
    "w4_033",
    "w5a_048",
    "w5b_051",
}
INTERSECTION_CLICKBENCH = {
    "w1_001", "w1_002", "w1_003", "w1_004", "w1_005", "w1_006",
    "w2_007", "w2_008", "w2_009", "w2_010",
    "w3_011", "w3_012", "w3_013", "w3_014", "w3_015",
    "w4_021", "w4_022", "w4_023", "w4_024", "w4_025",
    "w6_016", "w6_017", "w6_018", "w6_019", "w6_020",
}
INTERSECTION = {"h2o": INTERSECTION_H2O, "tpch": INTERSECTION_TPCH, "clickbench": INTERSECTION_CLICKBENCH}

BENCH_SCALES = {
    "h2o": ["sf1", "sf2", "sf4"],
    "tpch": ["sf1", "sf5", "sf10"],
    "clickbench": ["sf1", "sf5", "sf10", "sf20"],
}


def load_h2o_data(con, data_dir: Path, sf: str):
    csv_path = data_dir / "h2o" / sf / "groupby.csv"
    if not csv_path.exists():
        return False
    con.execute(f"DROP TABLE IF EXISTS groupby")
    con.execute(f"CREATE TABLE groupby AS SELECT * FROM read_csv_auto('{csv_path}')")
    return True


def load_tpch_data(con, data_dir: Path, sf: str):
    sf_dir = data_dir / "tpch" / sf
    if not sf_dir.exists():
        return False
    tables = ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"]
    for t in tables:
        csv_path = sf_dir / f"{t}.csv"
        if csv_path.exists():
            con.execute(f"DROP TABLE IF EXISTS {t}")
            con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_csv_auto('{csv_path}')")
    return True


def load_clickbench_data(con, data_dir: Path, sf: str):
    csv_path = data_dir / "clickbench" / sf / "t.csv"
    if not csv_path.exists():
        return False
    con.execute(f"DROP TABLE IF EXISTS hits")
    con.execute(f"CREATE TABLE hits AS SELECT * FROM read_csv_auto('{csv_path}')")
    return True


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


def run_query(con, sql, n_reps):
    """Run a SQL query n_reps times, return list of elapsed_ms."""
    times = []
    for _ in range(n_reps):
        t0 = time.time()
        try:
            con.execute(sql)
            _ = con.fetchall()
        except Exception as e:
            return None, str(e)
        times.append(round((time.time() - t0) * 1000, 1))
    return times, None


def main():
    parser = argparse.ArgumentParser(description="Run microbench via DuckDB")
    parser.add_argument("--data-dir", type=str, required=True)
    parser.add_argument("--microbench-dir", type=str, default="/workspace/gpu_db/microbench")
    parser.add_argument("--output-dir", type=str, required=True)
    parser.add_argument("--n-reps", type=int, default=3)
    parser.add_argument("--sample-interval", type=int, default=50)
    parser.add_argument("--test", action="store_true",
                        help="Quick test: 1 query per benchmark at smallest SF")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    microbench_dir = Path(args.microbench_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sampler = GPUSampler(interval_ms=args.sample_interval)

    timing_fields = ["engine", "benchmark", "scale", "query_file", "query_id", "workload",
                     "min_ms", "avg_ms", "reps", "error"]
    sample_fields = ["engine", "benchmark", "scale", "query_file", "time_offset_ms",
                     "power_w", "gpu_util_pct", "mem_used_mb", "pcie_gen"]

    timing_path = output_dir / "microbench_duckdb_timing.csv"
    samples_path = output_dir / "microbench_duckdb_metrics.csv"

    # Remove old
    for p in [timing_path, samples_path]:
        if p.exists():
            p.unlink()

    benchmarks = {
        "h2o": {"loader": load_h2o_data, "dir": "h2o"},
        "tpch": {"loader": load_tpch_data, "dir": "tpch"},
        "clickbench": {"loader": load_clickbench_data, "dir": "clickbench"},
    }

    print(f"DuckDB Microbench Runner")
    print(f"Reps: {args.n_reps}" + (" [TEST MODE]" if args.test else ""))
    print("=" * 60)

    for bench_name, bench_info in benchmarks.items():
        bench_dir = microbench_dir / bench_info["dir"]
        if not bench_dir.exists():
            continue

        # Filter SQL files to intersection queries only
        allowed = INTERSECTION.get(bench_name, set())
        all_sql = sorted([f for f in bench_dir.glob("w*.sql")])
        sql_files = []
        for f in all_sql:
            parts = f.stem.split("_", 2)
            qid = parts[0] + "_" + parts[1] if len(parts) >= 2 else ""
            if qid in allowed:
                sql_files.append(f)

        if not sql_files:
            continue

        scales = BENCH_SCALES.get(bench_name, ["sf1"])
        if args.test:
            scales = [scales[0]]
            sql_files = [sql_files[0]]

        for sf in scales:
            print(f"\n### {bench_name.upper()} {sf} ({len(sql_files)} queries)")

            con = duckdb.connect(":memory:")

            loaded = bench_info["loader"](con, data_dir, sf)
            if not loaded:
                print(f"  Data not found for {bench_name} {sf}, skipping")
                con.close()
                continue

            for sql_file in sql_files:
                fname = sql_file.stem
                parts = fname.split("_", 2)
                workload = parts[0] if parts else "?"
                query_id = parts[1] if len(parts) > 1 else "?"

                sql_text = sql_file.read_text().strip()
                sql_lines = [l for l in sql_text.split("\n") if not l.strip().startswith("--")]
                sql = "\n".join(sql_lines).strip()

                if not sql:
                    continue

                sampler.start(engine="duckdb", benchmark=bench_name,
                             scale=sf, query_file=fname)

                times, error = run_query(con, sql, args.n_reps)

                samples = sampler.stop()

                if times:
                    min_ms = min(times)
                    avg_ms = round(sum(times) / len(times), 1)
                    reps_str = ",".join(str(t) for t in times)
                    print(f"  {fname}: min={min_ms}ms avg={avg_ms}ms")
                else:
                    min_ms = avg_ms = 0
                    reps_str = ""
                    print(f"  {fname}: FAIL - {error}")

                write_header = not timing_path.exists()
                with open(timing_path, "a", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=timing_fields)
                    if write_header:
                        w.writeheader()
                    w.writerow({
                        "engine": "duckdb", "benchmark": bench_name, "scale": sf,
                        "query_file": fname, "query_id": query_id, "workload": workload,
                        "min_ms": min_ms, "avg_ms": avg_ms, "reps": reps_str,
                        "error": error or "",
                    })

                if samples:
                    write_header = not samples_path.exists()
                    with open(samples_path, "a", newline="") as f:
                        w = csv.DictWriter(f, fieldnames=sample_fields)
                        if write_header:
                            w.writeheader()
                        w.writerows(samples)

            con.close()

    print(f"\n{'=' * 60}")
    print(f"Done. Results in {output_dir}")


if __name__ == "__main__":
    main()
