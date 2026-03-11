#!/usr/bin/env python3
"""
Sirius (DuckDB GPU extension) steady-state metrics measurement.

Measures GPU and CPU power consumption for each query under sustained load.
Methodology:
  1. Each query runs in its own DuckDB process with gpu_buffer_init
  2. Calibration: 1 pass to measure base latency
  3. n_reps calculated so total execution >= TARGET_TIME_S (default 10s)
  4. nvidia-smi + RAPL sampled at 50ms intervals during sustained execution
  5. Steady-state detected via GPU utilization threshold

Usage:
    python run_sirius_metrics.py [tpch] [h2o] [clickbench]
    python run_sirius_metrics.py --target-time 10 --results-dir ./results tpch

Output:
    - sirius_*_metrics_summary.csv: per-query metrics
    - sirius_*_metrics_samples.csv: raw power samples at 50ms
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import re
import subprocess
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

# ── Defaults ─────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
MAXIMUS_DIR = SCRIPT_DIR.parent.parent
DEFAULT_SIRIUS_DIR = Path(os.environ.get("SIRIUS_DIR", "/home/xzw/sirius"))
DEFAULT_RESULTS_DIR = MAXIMUS_DIR / "results"

LD_EXTRA_SIRIUS = [
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libkvikio/lib64",
]
_ld = os.environ.get("LD_LIBRARY_PATH", "")
os.environ["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA_SIRIUS) + (":" + _ld if _ld else "")

SIRIUS_DATA_DIR = Path(os.environ.get("SIRIUS_DATA_DIR", "/home/xzw"))

BUFFER_INIT = 'call gpu_buffer_init("10 GB", "5 GB");'
QUERY_TIMEOUT_S = 120
TARGET_TIME_S = 60
MIN_REPS = 3
CALIBRATION_REPS = 3  # run 3 reps in calibration to separate buffer_init from query time
GPU_ID = "1"  # RTX 5080

BENCHMARKS = {
    "tpch": {
        "db_dir": SIRIUS_DATA_DIR / "tpch_duckdb",
        "db_pattern": "tpch_sf{sf}.duckdb",
        "query_dir": SIRIUS_DATA_DIR / "tpch_sql" / "queries" / "1",
        "scale_factors": [1, 2],
    },
    "h2o": {
        "db_dir": SIRIUS_DATA_DIR / "h2o_duckdb",
        "db_pattern": "h2o_{sf}.duckdb",
        "query_dir": SIRIUS_DATA_DIR / "h2o_sql" / "queries" / "1",
        "scale_factors": ["1gb", "2gb"],
    },
    "clickbench": {
        "db_dir": SIRIUS_DATA_DIR / "click_duckdb",
        "db_pattern": "clickbench_{sf}.duckdb",
        "query_dir": SIRIUS_DATA_DIR / "click_sql" / "queries" / "1",
        "scale_factors": [5],
    },
}

RE_RUN_TIME = re.compile(r"Run Time \(s\):\s*real\s+([\d.]+)", re.IGNORECASE)

# ── RAPL CPU power ───────────────────────────────────────────────────────────
RAPL_PKG_PATHS = []
RAPL_DRAM_PATHS = []
for _d in sorted(Path("/sys/class/powercap").glob("intel-rapl:*")):
    if _d.is_dir() and (_d / "energy_uj").exists():
        _nf = _d / "name"
        if _nf.exists() and _nf.read_text().strip().startswith("package"):
            RAPL_PKG_PATHS.append(_d / "energy_uj")
        for _sub in sorted(_d.glob("intel-rapl:*")):
            if _sub.is_dir() and (_sub / "name").exists():
                if (_sub / "name").read_text().strip() == "dram":
                    RAPL_DRAM_PATHS.append(_sub / "energy_uj")


def read_rapl_uj():
    pkg = sum(int(p.read_text().strip()) for p in RAPL_PKG_PATHS) if RAPL_PKG_PATHS else 0
    dram = sum(int(p.read_text().strip()) for p in RAPL_DRAM_PATHS) if RAPL_DRAM_PATHS else 0
    return pkg, dram


def load_queries(query_dir: Path):
    """Load SQL files: extract gpu_processing() calls."""
    queries = []
    for sql_file in sorted(query_dir.glob("*.sql")):
        qname = sql_file.stem
        lines = sql_file.read_text().strip().splitlines()
        gpu_lines = [l.strip() for l in lines if l.strip().startswith("call gpu_processing(")]
        if gpu_lines:
            queries.append((qname, gpu_lines))
    return queries


def build_metrics_sql(qname, gpu_lines, n_reps, buffer_init=BUFFER_INIT):
    """Build SQL: timer on, gpu_buffer_init, then N repetitions of the query."""
    parts = [".timer on", buffer_init]
    parts.append(f".print ===MARKER {qname}===")
    for _ in range(n_reps):
        parts.extend(gpu_lines)
    parts.append(".print ===END===")
    return "\n".join(parts) + "\n"


def parse_query_times(stdout: str) -> list[float]:
    """Parse Run Time entries from stdout, return list of times in seconds."""
    return [float(m.group(1)) for m in RE_RUN_TIME.finditer(stdout)]


def sample_gpu_metrics(stop_event, samples, interval=0.05):
    """Sample GPU + CPU metrics at 50ms intervals."""
    start = time.time()
    prev_pkg, prev_dram = read_rapl_uj()
    prev_time = start
    while not stop_event.is_set():
        try:
            r = subprocess.run(
                ["nvidia-smi", "-i", GPU_ID,
                 "--query-gpu=power.draw,utilization.gpu,memory.used,pcie.link.gen.current",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5
            )
            now = time.time()
            cur_pkg, cur_dram = read_rapl_uj()
            dt = now - prev_time
            cpu_pkg_w = (cur_pkg - prev_pkg) / 1e6 / dt if dt > 0 else 0
            cpu_dram_w = (cur_dram - prev_dram) / 1e6 / dt if dt > 0 else 0
            prev_pkg, prev_dram, prev_time = cur_pkg, cur_dram, now

            if r.returncode == 0:
                line = r.stdout.strip()
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 3:
                    samples.append({
                        "time_offset_ms": int((now - start) * 1000),
                        "power_w": float(parts[0]),
                        "gpu_util_pct": float(parts[1]),
                        "mem_used_mb": float(parts[2]),
                        "cpu_pkg_power_w": round(cpu_pkg_w, 1),
                        "cpu_dram_power_w": round(cpu_dram_w, 1),
                    })
        except Exception:
            pass
        stop_event.wait(interval)


def run_sirius_query(duckdb_bin, db_path, qname, gpu_lines, n_reps, buffer_init, timeout):
    """Run a single query N times in one DuckDB process, return (stdout, elapsed, rc)."""
    sql = build_metrics_sql(qname, gpu_lines, n_reps, buffer_init)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql)
        tmp = f.name
    try:
        t0 = time.time()
        r = subprocess.run(
            [str(duckdb_bin), str(db_path)],
            stdin=open(tmp, "r"),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=timeout,
        )
        elapsed = time.time() - t0
        return r.stdout or "", elapsed, r.returncode
    except subprocess.TimeoutExpired:
        return "", time.time() - t0, -1
    except Exception as e:
        return str(e), time.time() - t0, -2
    finally:
        os.unlink(tmp)


def main():
    parser = argparse.ArgumentParser(description="Sirius GPU steady-state metrics")
    parser.add_argument("benchmarks", nargs="*", default=["tpch", "h2o", "clickbench"])
    parser.add_argument("--sirius-dir", type=str, default=str(DEFAULT_SIRIUS_DIR))
    parser.add_argument("--results-dir", type=str, default=str(DEFAULT_RESULTS_DIR))
    parser.add_argument("--target-time", type=float, default=TARGET_TIME_S)
    parser.add_argument("--buffer-init", type=str, default=BUFFER_INIT)
    args = parser.parse_args()

    sirius_dir = Path(args.sirius_dir)
    duckdb_bin = sirius_dir / "build" / "release" / "duckdb"
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_bin.exists():
        print(f"ERROR: Sirius DuckDB binary not found: {duckdb_bin}")
        import sys; sys.exit(1)

    target_time_s = args.target_time
    buffer_init = args.buffer_init

    print("=" * 70)
    print("  SIRIUS GPU STEADY-STATE METRICS")
    print(f"  Target: {target_time_s}s sustained execution per query")
    print(f"  Started: {datetime.now()}")
    print("=" * 70)

    for bench_name in args.benchmarks:
        if bench_name not in BENCHMARKS:
            print(f"Unknown benchmark: {bench_name}, skipping")
            continue
        cfg = BENCHMARKS[bench_name]
        queries = load_queries(cfg["query_dir"])

        for sf in cfg["scale_factors"]:
            db_path = cfg["db_dir"] / cfg["db_pattern"].format(sf=sf)
            if not db_path.exists():
                print(f"[SKIP] {bench_name} SF={sf}: {db_path} not found")
                continue

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            tag = f"{bench_name}_sf{sf}"

            print(f"\n{'=' * 70}")
            print(f"  METRICS: {bench_name.upper()} SF={sf} ({len(queries)} queries)")
            print(f"  DB: {db_path}")
            print(f"{'=' * 70}")

            # Phase 1: Calibration - run CALIBRATION_REPS reps to separate
            # buffer_init time from per-query time
            print(f"\n--- Phase 1: Calibration ({CALIBRATION_REPS} reps each) ---")
            calibration = {}
            for qname, gpu_lines in queries:
                print(f"  {qname}...", end=" ", flush=True)
                stdout, elapsed, rc = run_sirius_query(
                    duckdb_bin, db_path, qname, gpu_lines, CALIBRATION_REPS,
                    buffer_init, QUERY_TIMEOUT_S)
                times = parse_query_times(stdout)
                has_fallback = "fallback" in stdout.lower()

                if rc == -1:
                    calibration[qname] = {"time_s": -1, "query_time_s": -1, "status": "TIMEOUT"}
                    print("TIMEOUT")
                elif not times or has_fallback:
                    calibration[qname] = {"time_s": -1, "query_time_s": -1,
                                          "status": "FALLBACK" if has_fallback else "FAIL"}
                    print(f"{'FALLBACK' if has_fallback else 'FAIL'}")
                else:
                    # times[0] = buffer_init, times[1] = 1st query (with data transfer),
                    # times[2:] = cached queries (GPU-only compute)
                    total = sum(times)
                    query_times = times[1:]  # exclude buffer_init
                    if len(query_times) >= 2:
                        # Use min of cached runs (times[2:]) as per-query time
                        cached_times = query_times[1:]
                        per_query = min(cached_times) if cached_times else query_times[0]
                    else:
                        per_query = query_times[0] if query_times else total
                    calibration[qname] = {
                        "time_s": total,
                        "query_time_s": per_query,
                        "status": "OK",
                    }
                    print(f"total={total:.3f}s, per_query={per_query:.4f}s "
                          f"(1st={query_times[0]:.4f}s)")

            # Phase 2: Calculate n_reps based on PER-QUERY time (not total incl. buffer_init)
            print(f"\n--- Phase 2: Calculate n_reps (target={target_time_s}s) ---")
            for qname, _ in queries:
                cal = calibration[qname]
                if cal["query_time_s"] > 0 and cal["status"] == "OK":
                    cal["n_reps"] = max(MIN_REPS, math.ceil(target_time_s / cal["query_time_s"]))
                else:
                    cal["n_reps"] = 0  # skip failed queries
                est_s = cal["n_reps"] * max(cal["query_time_s"], 0)
                print(f"  {qname}: {cal['query_time_s']:.4f}s x {cal['n_reps']} reps = {est_s:.1f}s "
                      f"({cal['status']})")

            # Phase 3: Metrics run with nvidia-smi + RAPL sampling
            print(f"\n--- Phase 3: Metrics run with power sampling ---")
            all_samples = []
            summaries = []

            for qname, gpu_lines in queries:
                cal = calibration[qname]
                if cal["n_reps"] == 0:
                    print(f"  {qname}: SKIP ({cal['status']})")
                    summaries.append({
                        "run_id": f"{tag}_{qname}", "benchmark": bench_name,
                        "sf": sf, "query": qname, "n_reps": 0,
                        "min_s": "", "avg_s": "", "elapsed_s": "",
                        "num_samples": 0, "num_steady_samples": 0,
                        "avg_power_w": "", "max_power_w": "", "max_mem_mb": "",
                        "avg_gpu_util": "", "max_gpu_util": "", "gpu_energy_j": "",
                        "avg_cpu_pkg_w": "", "avg_cpu_dram_w": "", "cpu_energy_j": "",
                        "status": cal["status"],
                    })
                    continue

                n_reps = cal["n_reps"]
                print(f"  {qname} ({n_reps} reps/pass, target={target_time_s}s)...",
                      end=" ", flush=True)

                # Start sampling
                samples = []
                stop_event = threading.Event()
                sampler = threading.Thread(target=sample_gpu_metrics,
                                           args=(stop_event, samples, 0.05))
                sampler.start()

                # Multi-pass: run DuckDB processes in a loop until target
                # wall time is reached. Each pass does buffer_init + n_reps
                # queries. This ensures we sample long enough for stable
                # nvidia-smi readings even when per-query time is very short.
                all_times = []
                total_elapsed = 0.0
                n_passes = 0
                status = "OK"
                per_pass_timeout = max(QUERY_TIMEOUT_S, target_time_s * 3)

                while total_elapsed < target_time_s:
                    stdout, pass_elapsed, rc = run_sirius_query(
                        duckdb_bin, db_path, qname, gpu_lines, n_reps,
                        buffer_init, per_pass_timeout)
                    pass_times = parse_query_times(stdout)
                    has_fallback = "fallback" in stdout.lower()

                    if rc == -1:
                        status = "TIMEOUT"
                        break
                    elif has_fallback:
                        status = "FALLBACK"
                        break
                    elif not pass_times:
                        status = "FAIL"
                        break

                    # Keep cached query times (skip buffer_init + first query)
                    if len(pass_times) > 2:
                        all_times.extend(pass_times[2:])
                    elif pass_times:
                        all_times.extend(pass_times)

                    total_elapsed += pass_elapsed
                    n_passes += 1

                stop_event.set()
                sampler.join(timeout=5)

                times = all_times
                elapsed = total_elapsed
                total_n_reps = n_reps * n_passes

                # Tag samples
                run_id = f"{tag}_{qname}"
                for s in samples:
                    s["run_id"] = run_id
                    s["sf"] = sf
                    s["query"] = qname
                all_samples.extend(samples)

                # Compute steady-state metrics
                if samples:
                    all_util = [s["gpu_util_pct"] for s in samples]
                    avg_util_all = sum(all_util) / len(all_util)
                    start_idx = 0
                    for i, s in enumerate(samples):
                        if s["gpu_util_pct"] >= avg_util_all:
                            start_idx = i
                            break
                    end_idx = len(samples) - 1
                    for i in range(len(samples) - 1, -1, -1):
                        if samples[i]["gpu_util_pct"] >= avg_util_all:
                            end_idx = i
                            break
                    steady = samples[start_idx:end_idx + 1] if end_idx >= start_idx else samples
                else:
                    steady = []

                if steady:
                    avg_power = sum(s["power_w"] for s in steady) / len(steady)
                    max_power = max(s["power_w"] for s in steady)
                    max_mem = max(s["mem_used_mb"] for s in steady)
                    avg_util = sum(s["gpu_util_pct"] for s in steady) / len(steady)
                    max_util = max(s["gpu_util_pct"] for s in steady)
                    avg_cpu_pkg_w = sum(s.get("cpu_pkg_power_w", 0) for s in steady) / len(steady)
                    avg_cpu_dram_w = sum(s.get("cpu_dram_power_w", 0) for s in steady) / len(steady)
                else:
                    avg_power = max_power = max_mem = avg_util = max_util = 0
                    avg_cpu_pkg_w = avg_cpu_dram_w = 0

                min_s = min(times) if times else 0
                avg_s = sum(times) / len(times) if times else 0
                gpu_energy_j = avg_power * min_s if min_s > 0 else 0
                cpu_energy_j = avg_cpu_pkg_w * min_s if min_s > 0 else 0

                summaries.append({
                    "run_id": run_id, "benchmark": bench_name,
                    "sf": sf, "query": qname, "n_reps": total_n_reps,
                    "min_s": f"{min_s:.4f}", "avg_s": f"{avg_s:.4f}",
                    "elapsed_s": f"{elapsed:.2f}",
                    "num_samples": len(samples),
                    "num_steady_samples": len(steady),
                    "avg_power_w": f"{avg_power:.1f}",
                    "max_power_w": f"{max_power:.1f}",
                    "max_mem_mb": f"{max_mem:.0f}",
                    "avg_gpu_util": f"{avg_util:.1f}",
                    "max_gpu_util": f"{max_util:.0f}",
                    "gpu_energy_j": f"{gpu_energy_j:.2f}",
                    "avg_cpu_pkg_w": f"{avg_cpu_pkg_w:.1f}",
                    "avg_cpu_dram_w": f"{avg_cpu_dram_w:.1f}",
                    "cpu_energy_j": f"{cpu_energy_j:.2f}",
                    "status": status,
                })

                print(f"{min_s:.3f}s, {elapsed:.1f}s ({n_passes} passes), "
                      f"GPU:{avg_power:.0f}W CPU:{avg_cpu_pkg_w:.0f}W, "
                      f"{max_util:.0f}%util, {max_mem:.0f}MB, GPU_E:{gpu_energy_j:.1f}J "
                      f"CPU_E:{cpu_energy_j:.1f}J [{status}]")

            # Save results
            samples_file = results_dir / f"sirius_{tag}_metrics_samples_{ts}.csv"
            with open(samples_file, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=[
                    "run_id", "sf", "query", "time_offset_ms",
                    "power_w", "gpu_util_pct", "mem_used_mb",
                    "cpu_pkg_power_w", "cpu_dram_power_w",
                ])
                w.writeheader()
                w.writerows(all_samples)

            summary_file = results_dir / f"sirius_{tag}_metrics_summary_{ts}.csv"
            with open(summary_file, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=[
                    "run_id", "benchmark", "sf", "query", "n_reps",
                    "min_s", "avg_s", "elapsed_s",
                    "num_samples", "num_steady_samples",
                    "avg_power_w", "max_power_w", "max_mem_mb",
                    "avg_gpu_util", "max_gpu_util", "gpu_energy_j",
                    "avg_cpu_pkg_w", "avg_cpu_dram_w", "cpu_energy_j", "status",
                ])
                w.writeheader()
                w.writerows(summaries)

            print(f"\n  Samples: {samples_file} ({len(all_samples)} samples)")
            print(f"  Summary: {summary_file} ({len(summaries)} queries)")

            ok_count = sum(1 for s in summaries if s["status"] == "OK")
            print(f"  --- {ok_count}/{len(queries)} OK")

    print(f"\n{'=' * 70}")
    print(f"  ALL DONE — {datetime.now()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
