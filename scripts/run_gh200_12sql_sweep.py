#!/usr/bin/env python3
"""
GH200 Category C v2 Experiment: 12 SQL × 12 GPU Configs.

Queries:
  TPC-H SF=10: q1, q3, q6, q9, q13, q16
  H2O SF=4gb:  q1, q3, q5, q6, q7, q9

GPU Configs (12):
  PL Sweep (auto SM, 6):  150W, 200W, 250W, 300W, 400W, 900W(baseline)
  SM Sweep (PL=900W, 4):  auto, 600MHz, 1110MHz, 1980MHz
  Cross (2):              200W+1110MHz, 300W+600MHz

Engines: Maximus + Sirius
Storage: gpu-resident + cpu-resident

Phase 1: Timing (3 runs, take 3rd)
Phase 2: Metrics (sustained execution, GPU util filtering, energy)
"""
from __future__ import annotations

import csv
import json
import math
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
BENCH_SCRIPTS = SCRIPT_DIR.parent / "benchmarks" / "scripts"
sys.path.insert(0, str(BENCH_SCRIPTS))

from hw_detect import (
    detect_gpu, maximus_data_dir, sirius_db_path, sirius_query_dir,
    buffer_init_sql, set_gpu_power_limit, set_gpu_sm_clock,
    reset_gpu_clocks, restore_gpu_defaults, MAXIMUS_DIR,
)

# ── Paths ─────────────────────────────────────────────────────────────────────
MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"
SIRIUS_DUCKDB = MAXIMUS_DIR / "sirius" / "build" / "release" / "duckdb"

# ── Library paths (Maximus=conda cuDF, Sirius=pip cuDF, different ABI) ────────
_conda_lib = Path(os.path.expanduser("~/miniconda3/envs/maximus_gpu/lib"))
_arrow_lib = Path(os.path.expanduser("~/arrow_install/lib"))
_user_site = Path(os.path.expanduser("~/.local/lib/python3.10/site-packages"))

_arrow_paths = [str(_arrow_lib)] if _arrow_lib.exists() else []

LD_MAXIMUS = ([str(_conda_lib)] if _conda_lib.exists() else []) + _arrow_paths
LD_SIRIUS = [
    str(p) for sub in [
        "libkvikio/lib64", "libcudf/lib64", "librmm/lib64",
        "rapids_logger/lib64", "nvidia/libnvcomp/lib64",
    ] if (p := _user_site / sub).exists()
] + _arrow_paths

# ── GPU Detection ─────────────────────────────────────────────────────────────
GPU_INFO = detect_gpu()
GPU_ID = GPU_INFO["index"]
GPU_ID_STR = str(GPU_ID)
VRAM_MB = GPU_INFO["vram_mb"]

# ── Experiment Definition ─────────────────────────────────────────────────────
QUERIES = {
    ("tpch", 10): ["q1", "q3", "q6", "q9", "q13", "q16"],
    ("h2o", "4gb"): ["q1", "q3", "q5", "q6", "q7", "q9"],
}

# 12 GPU configs: (power_limit_w, sm_clock_mhz)  sm_mhz=0 means auto
GPU_CONFIGS = [
    # PL sweep (auto SM) — 6 configs
    (150, 0),     # 1. Far below peak
    (200, 0),     # 2. Critical point on A100
    (250, 0),     # 3. Above most query avg power
    (300, 0),     # 4. GH200 idle region
    (400, 0),     # 5. Medium
    (900, 0),     # 6. Baseline (GH200 max)
    # SM sweep (PL=900W) — 4 configs
    (900, 0),     # 7. auto (same as #6, but explicit for SM sweep group)
    (900, 600),   # 8. Low freq
    (900, 1110),  # 9. Mid freq
    (900, 1980),  # 10. Max freq
    # Cross configs — 2 configs
    (200, 1110),  # 11. Low PL + mid freq (energy-optimal candidate)
    (300, 600),   # 12. Low freq + higher PL
]

# Deduplicate (config 6 and 7 are the same)
_seen = set()
GPU_CONFIGS_DEDUP = []
for cfg in GPU_CONFIGS:
    if cfg not in _seen:
        _seen.add(cfg)
        GPU_CONFIGS_DEDUP.append(cfg)
GPU_CONFIGS = GPU_CONFIGS_DEDUP

ENGINES = ["maximus", "sirius"]
STORAGE_MODES = ["gpu", "cpu"]

# Metrics constants
TARGET_TIME_S = 5
MIN_REPS = 3
MAX_REPS = 100


# ── Environment ───────────────────────────────────────────────────────────────
def get_env(engine="maximus"):
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    extra = LD_MAXIMUS if engine == "maximus" else LD_SIRIUS
    env["LD_LIBRARY_PATH"] = ":".join(extra) + (":" + ld if ld else "")
    return env


# ── RAPL ──────────────────────────────────────────────────────────────────────
RAPL_PKG_PATHS = []
for d in sorted(Path("/sys/class/powercap").glob("intel-rapl:*")):
    if d.is_dir() and (d / "energy_uj").exists():
        name_file = d / "name"
        if name_file.exists() and name_file.read_text().strip().startswith("package"):
            RAPL_PKG_PATHS.append(d / "energy_uj")


def read_rapl_uj():
    return sum(int(p.read_text().strip()) for p in RAPL_PKG_PATHS) if RAPL_PKG_PATHS else 0


# ── GPU Sampling ──────────────────────────────────────────────────────────────
def sample_gpu(stop_event, samples, interval=0.05):
    start = time.time()
    prev_pkg = read_rapl_uj()
    prev_time = start
    while not stop_event.is_set():
        try:
            r = subprocess.run(
                ["nvidia-smi", "-i", GPU_ID_STR,
                 "--query-gpu=power.draw,utilization.gpu,memory.used,clocks.current.sm",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5)
            now = time.time()
            cur_pkg = read_rapl_uj()
            dt = now - prev_time
            cpu_w = (cur_pkg - prev_pkg) / 1e6 / dt if dt > 0 else 0
            prev_pkg, prev_time = cur_pkg, now
            if r.returncode == 0:
                parts = [p.strip() for p in r.stdout.strip().split(",")]
                if len(parts) >= 4:
                    samples.append({
                        "t_ms": int((now - start) * 1000),
                        "power_w": float(parts[0]),
                        "gpu_util": float(parts[1]),
                        "mem_mb": float(parts[2]),
                        "sm_mhz": float(parts[3]),
                        "cpu_w": round(cpu_w, 1),
                    })
        except Exception:
            pass
        stop_event.wait(interval)


def steady_state(samples):
    if not samples:
        return {"avg_power_w": 0, "max_power_w": 0, "avg_util": 0,
                "max_mem_mb": 0, "avg_sm_mhz": 0, "avg_cpu_w": 0, "n_steady": 0}
    utils = [s["gpu_util"] for s in samples]
    avg_u = sum(utils) / len(utils)
    si = next((i for i, s in enumerate(samples) if s["gpu_util"] >= avg_u), 0)
    ei = next((i for i in range(len(samples)-1, -1, -1) if samples[i]["gpu_util"] >= avg_u), len(samples)-1)
    ss = samples[si:ei+1] if ei >= si else samples
    n = len(ss)
    return {
        "avg_power_w": sum(s["power_w"] for s in ss) / n,
        "max_power_w": max(s["power_w"] for s in ss),
        "avg_util": sum(s["gpu_util"] for s in ss) / n,
        "max_mem_mb": max(s["mem_mb"] for s in ss),
        "avg_sm_mhz": sum(s["sm_mhz"] for s in ss) / n,
        "avg_cpu_w": sum(s["cpu_w"] for s in ss) / n,
        "n_steady": n,
    }


# ── Maximus Runner ────────────────────────────────────────────────────────────
def run_maxbench(benchmark, query, n_reps, sf, storage="gpu", timeout=300):
    data_path = maximus_data_dir(benchmark, sf)
    cmd = [
        str(MAXBENCH), "--benchmark", benchmark,
        "-q", query, "-d", "gpu", "-r", str(n_reps),
        "--n_reps_storage", "1", "--path", str(data_path),
        "-s", storage, "--engines", "maximus",
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=timeout, env=get_env("maximus"))
        return r.stdout + (r.stderr or ""), r.returncode
    except subprocess.TimeoutExpired:
        return "TIMEOUT", -1
    except Exception as e:
        return f"ERROR: {e}", -2


def parse_maxbench_times(output, query):
    pattern = rf"gpu,maximus,{re.escape(query)},([\d.,]+)"
    m = re.search(pattern, output)
    if m:
        return [float(t) for t in m.group(1).rstrip(",").split(",") if t.strip()]
    # Fallback: MAXIMUS TIMINGS format
    current = None
    for line in output.split("\n"):
        qm = re.match(r"\s*QUERY (\w+)\s*", line.strip())
        if qm:
            current = qm.group(1)
        tm = re.match(r"- MAXIMUS TIMINGS \[ms\]:\s*(.*)", line.strip())
        if tm and current == query:
            return [float(t.strip()) for t in tm.group(1).rstrip(",").split(",") if t.strip()]
    return []


# ── Sirius Runner ─────────────────────────────────────────────────────────────
def run_sirius(benchmark, sf, query, n_reps, n_warmup=2, timeout=120):
    db_path = sirius_db_path(benchmark, sf)
    sql_file = sirius_query_dir(benchmark) / f"{query}.sql"
    if not db_path.exists():
        return [], "NO_DB"
    if not sql_file.exists():
        return [], "NO_SQL"

    lines = [l.strip() for l in sql_file.read_text().strip().splitlines() if l.strip()]
    query_line = next((l for l in lines if "gpu_processing" in l), None)
    if not query_line:
        return [], "NO_GPU_PROCESSING"

    buf_init = buffer_init_sql(VRAM_MB)
    script = [buf_init]
    for _ in range(n_warmup):
        script.append(query_line)
    script.append(".timer on")
    for _ in range(n_reps):
        script.append(query_line)

    cmd = [str(SIRIUS_DUCKDB), str(db_path)]
    try:
        r = subprocess.run(cmd, input="\n".join(script), capture_output=True,
                           text=True, timeout=timeout, env=get_env("sirius"))
        output = r.stdout + (r.stderr or "")
    except subprocess.TimeoutExpired:
        return [], "TIMEOUT"
    except Exception as e:
        return [], f"ERROR"

    times = [float(m.group(1))
             for line in output.split("\n")
             if (m := re.search(r"Run Time \(s\):\s*real\s+([\d.]+)", line))]
    return times, ("OK" if times else "FAIL")


# ── GPU Config ────────────────────────────────────────────────────────────────
def apply_config(pl_w, sm_mhz):
    set_gpu_power_limit(GPU_ID, pl_w)
    if sm_mhz > 0:
        set_gpu_sm_clock(GPU_ID, sm_mhz)
    else:
        reset_gpu_clocks(GPU_ID)
    time.sleep(2)


def cfg_tag(pl_w, sm_mhz):
    return f"pl{pl_w}w_sm{'auto' if sm_mhz == 0 else sm_mhz}"


# ── Run One Query (timing or metrics) ────────────────────────────────────────
def run_query(engine, benchmark, sf, query, storage, n_reps, do_sample=False):
    """Run a query and optionally sample GPU metrics. Returns dict."""
    samples = []
    stop = threading.Event()
    sampler = None

    if do_sample:
        sampler = threading.Thread(target=sample_gpu, args=(stop, samples, 0.05))
        sampler.start()

    t0 = time.time()
    if engine == "maximus":
        output, rc = run_maxbench(benchmark, query, n_reps, sf,
                                  storage=storage, timeout=600)
        times_ms = parse_maxbench_times(output, query)
        status = "OK" if times_ms else ("TIMEOUT" if rc == -1 else "FAIL")
        if "out_of_memory" in (output or "").lower():
            status = "OOM"
    else:  # sirius
        times_s, status = run_sirius(benchmark, sf, query, n_reps=n_reps,
                                     n_warmup=2, timeout=600)
        times_ms = [t * 1000 for t in times_s]

    elapsed = time.time() - t0

    if sampler:
        stop.set()
        sampler.join(timeout=5)

    min_ms = min(times_ms) if times_ms else 0
    run3_ms = times_ms[2] if len(times_ms) >= 3 else (times_ms[-1] if times_ms else 0)
    ss = steady_state(samples) if samples else None

    return {
        "times_ms": times_ms,
        "min_ms": min_ms,
        "run3_ms": run3_ms,
        "elapsed_s": elapsed,
        "status": status,
        "samples": samples,
        "steady": ss,
    }


# ── Phase 1: Timing ──────────────────────────────────────────────────────────
def phase_timing(results_dir):
    timing_path = results_dir / "timing.csv"
    rows = []

    for pl_w, sm_mhz in GPU_CONFIGS:
        tag = cfg_tag(pl_w, sm_mhz)
        print(f"\n{'#'*70}")
        print(f"# TIMING: {tag} ({datetime.now().strftime('%H:%M:%S')})")
        print(f"{'#'*70}")
        apply_config(pl_w, sm_mhz)

        for (bench, sf), queries in QUERIES.items():
            for engine in ENGINES:
                for storage in STORAGE_MODES:
                    label = f"{engine}/{bench}/sf{sf}/{storage}"
                    print(f"\n  --- {label} ---")
                    for q in queries:
                        r = run_query(engine, bench, sf, q, storage, n_reps=3)
                        t3 = r["run3_ms"]
                        mn = r["min_ms"]
                        st = r["status"]
                        if st == "OK":
                            print(f"    {q}: 3rd={t3:.2f}ms min={mn:.2f}ms")
                        else:
                            print(f"    {q}: {st}")
                        rows.append({
                            "config": tag, "pl_w": pl_w,
                            "sm_mhz": sm_mhz if sm_mhz > 0 else "auto",
                            "engine": engine, "benchmark": bench,
                            "sf": sf, "query": q, "storage": storage,
                            "n_reps": len(r["times_ms"]),
                            "run3_ms": f"{t3:.4f}" if t3 else "",
                            "min_ms": f"{mn:.4f}" if mn else "",
                            "all_ms": ";".join(f"{t:.4f}" for t in r["times_ms"]),
                            "status": st,
                        })
                    sys.stdout.flush()

    # Save
    fields = ["config", "pl_w", "sm_mhz", "engine", "benchmark", "sf",
              "query", "storage", "n_reps", "run3_ms", "min_ms", "all_ms", "status"]
    with open(timing_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"\n[SAVED] {timing_path} ({len(rows)} rows)")
    return rows


# ── Phase 2: Metrics ─────────────────────────────────────────────────────────
def phase_metrics(results_dir, timing_rows):
    metrics_path = results_dir / "metrics.csv"
    samples_path = results_dir / "samples.csv"

    # Build timing lookup for n_reps calculation
    timing_lut = {}
    for r in timing_rows:
        if r["status"] == "OK" and r["min_ms"]:
            key = (r["config"], r["engine"], r["benchmark"],
                   str(r["sf"]), r["query"], r["storage"])
            timing_lut[key] = float(r["min_ms"])

    mrows = []
    all_samples = []

    for pl_w, sm_mhz in GPU_CONFIGS:
        tag = cfg_tag(pl_w, sm_mhz)
        print(f"\n{'#'*70}")
        print(f"# METRICS: {tag} ({datetime.now().strftime('%H:%M:%S')})")
        print(f"{'#'*70}")
        apply_config(pl_w, sm_mhz)

        for (bench, sf), queries in QUERIES.items():
            for engine in ENGINES:
                for storage in STORAGE_MODES:
                    label = f"{engine}/{bench}/sf{sf}/{storage}"
                    print(f"\n  --- {label} ---")
                    for q in queries:
                        key = (tag, engine, bench, str(sf), q, storage)
                        cal_ms = timing_lut.get(key, 0)
                        if cal_ms > 0:
                            n_reps = min(MAX_REPS, max(MIN_REPS,
                                math.ceil(TARGET_TIME_S * 1000 / cal_ms)))
                        else:
                            n_reps = MIN_REPS

                        print(f"    {q} ({n_reps}r)...", end=" ", flush=True)
                        r = run_query(engine, bench, sf, q, storage,
                                      n_reps=n_reps, do_sample=True)

                        ss = r["steady"] or {}
                        min_ms = r["min_ms"]
                        qt_s = min_ms / 1000 if min_ms > 0 else (
                            r["elapsed_s"] / n_reps if n_reps > 0 else 0)
                        gpu_e = ss.get("avg_power_w", 0) * qt_s
                        cpu_e = ss.get("avg_cpu_w", 0) * qt_s

                        # Tag and collect samples
                        run_id = f"{tag}_{engine}_{bench}_sf{sf}_{q}_{storage}"
                        for s in r["samples"]:
                            s["run_id"] = run_id
                        all_samples.extend(r["samples"])

                        mrows.append({
                            "config": tag, "pl_w": pl_w,
                            "sm_mhz": sm_mhz if sm_mhz > 0 else "auto",
                            "engine": engine, "benchmark": bench,
                            "sf": sf, "query": q, "storage": storage,
                            "n_reps": n_reps,
                            "min_ms": f"{min_ms:.4f}" if min_ms else "",
                            "query_ms": f"{qt_s*1000:.4f}",
                            "elapsed_s": f"{r['elapsed_s']:.2f}",
                            "n_samples": len(r["samples"]),
                            "n_steady": ss.get("n_steady", 0),
                            "avg_power_w": f"{ss.get('avg_power_w',0):.1f}",
                            "max_power_w": f"{ss.get('max_power_w',0):.1f}",
                            "avg_util": f"{ss.get('avg_util',0):.1f}",
                            "max_mem_mb": f"{ss.get('max_mem_mb',0):.0f}",
                            "avg_sm_mhz": f"{ss.get('avg_sm_mhz',0):.0f}",
                            "gpu_energy_j": f"{gpu_e:.4f}",
                            "cpu_energy_j": f"{cpu_e:.4f}",
                            "total_energy_j": f"{gpu_e+cpu_e:.4f}",
                            "status": r["status"],
                        })
                        if r["status"] == "OK":
                            print(f"{qt_s*1000:.1f}ms P={ss.get('avg_power_w',0):.0f}W "
                                  f"E={gpu_e:.3f}J u={ss.get('avg_util',0):.0f}%")
                        else:
                            print(r["status"])
                    sys.stdout.flush()

    # Save metrics
    mfields = ["config", "pl_w", "sm_mhz", "engine", "benchmark", "sf",
               "query", "storage", "n_reps", "min_ms", "query_ms", "elapsed_s",
               "n_samples", "n_steady", "avg_power_w", "max_power_w",
               "avg_util", "max_mem_mb", "avg_sm_mhz",
               "gpu_energy_j", "cpu_energy_j", "total_energy_j", "status"]
    with open(metrics_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=mfields)
        w.writeheader()
        w.writerows(mrows)

    # Save raw samples
    if all_samples:
        sfields = ["run_id", "t_ms", "power_w", "gpu_util", "mem_mb", "sm_mhz", "cpu_w"]
        with open(samples_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=sfields)
            w.writeheader()
            w.writerows(all_samples)

    print(f"\n[SAVED] {metrics_path} ({len(mrows)} rows)")
    print(f"[SAVED] {samples_path} ({len(all_samples)} samples)")
    return mrows


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    results_dir = Path(MAXIMUS_DIR / "results" / "gh200_12sql_sweep")
    results_dir.mkdir(parents=True, exist_ok=True)

    n_queries = sum(len(qs) for qs in QUERIES.values())
    n_combos = len(GPU_CONFIGS) * n_queries * len(ENGINES) * len(STORAGE_MODES)

    print("=" * 70)
    print("  GH200 Category C v2: 12 SQL × 12 GPU Configs")
    print(f"  GPU: {GPU_INFO['name']} (idx={GPU_ID}, {VRAM_MB}MB)")
    print(f"  PL: {GPU_INFO['power_min_w']}-{GPU_INFO['power_max_w']}W")
    print(f"  SM: {GPU_INFO['sm_clocks'][0]}-{GPU_INFO['sm_clocks'][-1]}MHz")
    print(f"  Queries: {n_queries}")
    for (bench, sf), qs in QUERIES.items():
        print(f"    {bench} SF={sf}: {', '.join(qs)}")
    print(f"  Configs: {len(GPU_CONFIGS)} (deduped)")
    for i, (pl, sm) in enumerate(GPU_CONFIGS):
        print(f"    [{i+1:2d}] PL={pl}W SM={'auto' if sm==0 else f'{sm}MHz'}")
    print(f"  Engines: {ENGINES}")
    print(f"  Storage: {STORAGE_MODES}")
    print(f"  Total runs: {n_combos} (timing) + {n_combos} (metrics)")
    print(f"  Results: {results_dir}")
    print(f"  Started: {datetime.now()}")
    print("=" * 70)

    # Save config
    with open(results_dir / "config.json", "w") as f:
        json.dump({
            "gpu": GPU_INFO["name"], "vram_mb": VRAM_MB,
            "queries": {f"{b}_sf{sf}": qs for (b, sf), qs in QUERIES.items()},
            "configs": [{"pl_w": pl, "sm_mhz": sm} for pl, sm in GPU_CONFIGS],
            "engines": ENGINES, "storage": STORAGE_MODES,
            "started": datetime.now().isoformat(),
        }, f, indent=2)

    # Cleanup handler
    def cleanup(sig, frame):
        print("\n[INTERRUPTED] Restoring GPU...")
        restore_gpu_defaults(GPU_ID, GPU_INFO.get("power_default_w"))
        sys.exit(1)
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        print("\n" + "=" * 70)
        print("  PHASE 1: TIMING (3 runs, take 3rd)")
        print("=" * 70)
        timing_rows = phase_timing(results_dir)

        print("\n" + "=" * 70)
        print("  PHASE 2: METRICS (sustained execution + energy)")
        print("=" * 70)
        metrics_rows = phase_metrics(results_dir, timing_rows)

    finally:
        restore_gpu_defaults(GPU_ID, GPU_INFO.get("power_default_w"))

    print(f"\n{'='*70}")
    print(f"  COMPLETE: {datetime.now()}")
    print(f"  Timing:  {results_dir / 'timing.csv'}")
    print(f"  Metrics: {results_dir / 'metrics.csv'}")
    print(f"  Samples: {results_dir / 'samples.csv'}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
