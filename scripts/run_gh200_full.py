#!/usr/bin/env python3
"""
GH200 Full Benchmark: All queries, default GPU config (PL=900W, SM=auto).

Benchmarks:
  - tpch SF=1,10 (22 queries each)
  - h2o SF=1gb,2gb,3gb,4gb (9 queries each)
  - clickbench SF=10 (39 queries)

Engines: Maximus + Sirius
Storage: gpu + cpu

Phase 1: Timing (3 runs, take 3rd)
Phase 2: Metrics (sustained execution, GPU util filtering, energy)

Results saved incrementally per (benchmark, sf) so partial results are preserved.
"""
from __future__ import annotations

import csv
import json
import math
import os
import re
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
    detect_gpu, get_benchmark_config, maximus_data_dir,
    sirius_db_path, sirius_query_dir, buffer_init_sql, MAXIMUS_DIR,
)

MAXBENCH = MAXIMUS_DIR / "build" / "benchmarks" / "maxbench"
SIRIUS_DUCKDB = MAXIMUS_DIR / "sirius" / "build" / "release" / "duckdb"

# Library paths
_conda_lib = Path(os.path.expanduser("~/miniconda3/envs/maximus_gpu/lib"))
_arrow_lib = Path(os.path.expanduser("~/arrow_install/lib"))
_user_site = Path(os.path.expanduser("~/.local/lib/python3.10/site-packages"))
_arrow = [str(_arrow_lib)] if _arrow_lib.exists() else []

LD_MAXIMUS = ([str(_conda_lib)] if _conda_lib.exists() else []) + _arrow
LD_SIRIUS = [str(p) for sub in ["libkvikio/lib64","libcudf/lib64","librmm/lib64",
             "rapids_logger/lib64","nvidia/libnvcomp/lib64"]
             if (p := _user_site / sub).exists()] + _arrow

GPU_INFO = detect_gpu()
GPU_ID = GPU_INFO["index"]
GPU_ID_STR = str(GPU_ID)
VRAM_MB = GPU_INFO["vram_mb"]
BENCHMARKS = get_benchmark_config(VRAM_MB)

# Scope
EXPERIMENT = {
    "tpch": [1, 10],
    "h2o": ["1gb", "2gb", "3gb", "4gb"],
    "clickbench": [10],
}
ENGINES = ["maximus", "sirius"]
STORAGE = ["gpu", "cpu"]

TARGET_TIME_S = 5
MIN_REPS = 3
MAX_REPS = 100


def get_env(engine):
    env = os.environ.copy()
    ld = env.get("LD_LIBRARY_PATH", "")
    extra = LD_MAXIMUS if engine == "maximus" else LD_SIRIUS
    env["LD_LIBRARY_PATH"] = ":".join(extra) + (":" + ld if ld else "")
    return env


# ── RAPL ──────────────────────────────────────────────────────────────────
RAPL_PKG = [d / "energy_uj" for d in sorted(Path("/sys/class/powercap").glob("intel-rapl:*"))
            if d.is_dir() and (d/"energy_uj").exists() and (d/"name").exists()
            and (d/"name").read_text().strip().startswith("package")]

def read_rapl():
    return sum(int(p.read_text().strip()) for p in RAPL_PKG) if RAPL_PKG else 0


def sample_gpu(stop, samples, interval=0.05):
    t0 = time.time(); prev_r = read_rapl(); prev_t = t0
    while not stop.is_set():
        try:
            r = subprocess.run(["nvidia-smi","-i",GPU_ID_STR,
                "--query-gpu=power.draw,utilization.gpu,memory.used,clocks.current.sm",
                "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5)
            now = time.time(); cur_r = read_rapl(); dt = now-prev_t
            cpu_w = (cur_r-prev_r)/1e6/dt if dt>0 else 0
            prev_r, prev_t = cur_r, now
            if r.returncode == 0:
                p = [x.strip() for x in r.stdout.strip().split(",")]
                if len(p)>=4:
                    samples.append({"t_ms":int((now-t0)*1000),"power_w":float(p[0]),
                        "gpu_util":float(p[1]),"mem_mb":float(p[2]),
                        "sm_mhz":float(p[3]),"cpu_w":round(cpu_w,1)})
        except: pass
        stop.wait(interval)


def steady_state(samples):
    if not samples:
        return {"avg_pw":0,"max_pw":0,"avg_util":0,"max_mem":0,"avg_cpu_w":0,"n_ss":0}
    u = [s["gpu_util"] for s in samples]; au = sum(u)/len(u)
    si = next((i for i,s in enumerate(samples) if s["gpu_util"]>=au),0)
    ei = next((i for i in range(len(samples)-1,-1,-1) if samples[i]["gpu_util"]>=au),len(samples)-1)
    ss = samples[si:ei+1] if ei>=si else samples; n=len(ss)
    return {"avg_pw":sum(s["power_w"] for s in ss)/n, "max_pw":max(s["power_w"] for s in ss),
            "avg_util":sum(s["gpu_util"] for s in ss)/n, "max_mem":max(s["mem_mb"] for s in ss),
            "avg_cpu_w":sum(s["cpu_w"] for s in ss)/n, "n_ss":n}


# ── Maximus ───────────────────────────────────────────────────────────────
def run_maxbench(bench, query, n_reps, sf, storage="gpu", timeout=300):
    dp = maximus_data_dir(bench, sf)
    cmd = [str(MAXBENCH),"--benchmark",bench,"-q",query,"-d","gpu","-r",str(n_reps),
           "--n_reps_storage","1","--path",str(dp),"-s",storage,"--engines","maximus"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=get_env("maximus"))
        return r.stdout+(r.stderr or ""), r.returncode
    except subprocess.TimeoutExpired: return "TIMEOUT", -1
    except Exception as e: return f"ERROR: {e}", -2


def parse_times(output, query):
    m = re.search(rf"gpu,maximus,{re.escape(query)},([\d.,]+)", output)
    if m: return [float(t) for t in m.group(1).rstrip(",").split(",") if t.strip()]
    cur = None
    for line in output.split("\n"):
        qm = re.match(r"\s*QUERY (\w+)\s*", line.strip())
        if qm: cur = qm.group(1)
        tm = re.match(r"- MAXIMUS TIMINGS \[ms\]:\s*(.*)", line.strip())
        if tm and cur == query:
            return [float(t.strip()) for t in tm.group(1).rstrip(",").split(",") if t.strip()]
    return []


# ── Sirius ────────────────────────────────────────────────────────────────
def run_sirius(bench, sf, query, n_reps, n_warmup=2, timeout=120):
    db = sirius_db_path(bench, sf)
    sql_file = sirius_query_dir(bench) / f"{query}.sql"
    if not db.exists(): return [], "NO_DB"
    if not sql_file.exists(): return [], "NO_SQL"
    lines = [l.strip() for l in sql_file.read_text().strip().splitlines() if l.strip()]
    ql = next((l for l in lines if "gpu_processing" in l), None)
    if not ql: return [], "NO_GPU_PROC"
    buf = buffer_init_sql(VRAM_MB)
    script = [buf] + [ql]*n_warmup + [".timer on"] + [ql]*n_reps
    try:
        r = subprocess.run([str(SIRIUS_DUCKDB),str(db)], input="\n".join(script),
                           capture_output=True, text=True, timeout=timeout, env=get_env("sirius"))
        output = r.stdout + (r.stderr or "")
    except subprocess.TimeoutExpired: return [], "TIMEOUT"
    except: return [], "ERROR"
    times = [float(m.group(1)) for line in output.split("\n")
             if (m := re.search(r"Run Time \(s\):\s*real\s+([\d.]+)", line))]
    return times, ("OK" if times else "FAIL")


# ── Run query ─────────────────────────────────────────────────────────────
def run_query(engine, bench, sf, query, storage, n_reps, do_sample=False):
    samples = []; stop = threading.Event(); sampler = None
    if do_sample:
        sampler = threading.Thread(target=sample_gpu, args=(stop, samples, 0.05))
        sampler.start()
    t0 = time.time()
    if engine == "maximus":
        out, rc = run_maxbench(bench, query, n_reps, sf, storage=storage, timeout=600)
        tms = parse_times(out, query)
        status = "OK" if tms else ("TIMEOUT" if rc==-1 else "FAIL")
        if "out_of_memory" in (out or "").lower(): status = "OOM"
    else:
        ts, status = run_sirius(bench, sf, query, n_reps=n_reps, n_warmup=2, timeout=600)
        tms = [t*1000 for t in ts]
    elapsed = time.time() - t0
    if sampler: stop.set(); sampler.join(timeout=5)
    mn = min(tms) if tms else 0
    r3 = tms[2] if len(tms)>=3 else (tms[-1] if tms else 0)
    ss = steady_state(samples) if samples else None
    return {"times_ms":tms,"min_ms":mn,"run3_ms":r3,"elapsed_s":elapsed,
            "status":status,"samples":samples,"steady":ss}


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    results_dir = Path(MAXIMUS_DIR / "results" / "gh200_full")
    results_dir.mkdir(parents=True, exist_ok=True)

    queries_cfg = get_benchmark_config(VRAM_MB)

    print("=" * 70)
    print("  GH200 Full Benchmark (PL=900W, SM=auto)")
    print(f"  GPU: {GPU_INFO['name']} ({VRAM_MB}MB)")
    for bench, sfs in EXPERIMENT.items():
        nq = len(queries_cfg[bench]["queries"])
        print(f"  {bench}: SF={sfs}, {nq} queries")
    print(f"  Engines: {ENGINES}")
    print(f"  Storage: {STORAGE}")
    print(f"  Started: {datetime.now()}")
    print("=" * 70)

    all_timing = []
    all_metrics = []
    all_samples = []

    for bench, sfs in EXPERIMENT.items():
        queries = queries_cfg[bench]["queries"]
        for sf in sfs:
            tag = f"{bench}_sf{sf}"
            checkpoint_t = results_dir / f"timing_{tag}.csv"
            checkpoint_m = results_dir / f"metrics_{tag}.csv"

            # Skip if already done
            if checkpoint_t.exists() and checkpoint_m.exists():
                print(f"\n[SKIP] {tag} already done")
                # Load existing
                with open(checkpoint_t) as f: all_timing.extend(list(csv.DictReader(f)))
                with open(checkpoint_m) as f: all_metrics.extend(list(csv.DictReader(f)))
                continue

            print(f"\n{'#'*70}")
            print(f"# {tag.upper()} ({len(queries)} queries) — {datetime.now().strftime('%H:%M:%S')}")
            print(f"{'#'*70}")

            # ── Phase 1: Timing ───────────────────────────────────────
            t_rows = []
            for engine in ENGINES:
                for storage in STORAGE:
                    label = f"{engine}/{bench}/sf{sf}/{storage}"
                    print(f"\n  [TIMING] {label}")
                    ok = 0
                    for q in queries:
                        r = run_query(engine, bench, sf, q, storage, 3)
                        t3 = r["run3_ms"]; mn = r["min_ms"]; st = r["status"]
                        if st == "OK":
                            ok += 1
                            print(f"    {q}: 3rd={t3:.2f}ms min={mn:.2f}ms")
                        else:
                            print(f"    {q}: {st}")
                        t_rows.append({"engine":engine,"benchmark":bench,"sf":sf,
                            "query":q,"storage":storage,"n_reps":len(r["times_ms"]),
                            "run3_ms":f"{t3:.4f}" if t3 else "","min_ms":f"{mn:.4f}" if mn else "",
                            "all_ms":";".join(f"{t:.4f}" for t in r["times_ms"]),"status":st})
                    print(f"    --- {ok}/{len(queries)} OK")
                    sys.stdout.flush()

            # ── Validate timing ───────────────────────────────────────
            ok_t = sum(1 for r in t_rows if r["status"]=="OK")
            total_t = len(t_rows)
            print(f"\n  [CHECK] Timing {tag}: {ok_t}/{total_t} OK")
            # Quick sanity: check a few known queries
            for r in t_rows:
                if r["status"]=="OK" and r["min_ms"]:
                    ms = float(r["min_ms"])
                    if ms > 60000:
                        print(f"    WARNING: {r['engine']}/{r['query']}/{r['storage']} = {ms:.0f}ms (>60s)")

            # ── Phase 2: Metrics ──────────────────────────────────────
            # Build timing lookup for n_reps
            tim_lut = {}
            for r in t_rows:
                if r["status"]=="OK" and r["min_ms"]:
                    tim_lut[(r["engine"],r["benchmark"],str(r["sf"]),r["query"],r["storage"])] = float(r["min_ms"])

            m_rows = []
            for engine in ENGINES:
                for storage in STORAGE:
                    label = f"{engine}/{bench}/sf{sf}/{storage}"
                    print(f"\n  [METRICS] {label}")
                    for q in queries:
                        key = (engine, bench, str(sf), q, storage)
                        cal_ms = tim_lut.get(key, 0)
                        if cal_ms > 0:
                            n_reps = min(MAX_REPS, max(MIN_REPS, math.ceil(TARGET_TIME_S*1000/cal_ms)))
                        else:
                            n_reps = MIN_REPS

                        r = run_query(engine, bench, sf, q, storage, n_reps, do_sample=True)
                        ss = r["steady"] or {}
                        mn = r["min_ms"]
                        qt_s = mn/1000 if mn>0 else (r["elapsed_s"]/n_reps if n_reps>0 else 0)
                        gpu_e = ss.get("avg_pw",0)*qt_s
                        cpu_e = ss.get("avg_cpu_w",0)*qt_s

                        run_id = f"{engine}_{bench}_sf{sf}_{q}_{storage}"
                        for s in r["samples"]: s["run_id"] = run_id
                        all_samples.extend(r["samples"])

                        m_rows.append({"engine":engine,"benchmark":bench,"sf":sf,
                            "query":q,"storage":storage,"n_reps":n_reps,
                            "min_ms":f"{mn:.4f}" if mn else "",
                            "query_ms":f"{qt_s*1000:.4f}","elapsed_s":f"{r['elapsed_s']:.2f}",
                            "n_samples":len(r["samples"]),"n_steady":ss.get("n_ss",0),
                            "avg_power_w":f"{ss.get('avg_pw',0):.1f}",
                            "max_power_w":f"{ss.get('max_pw',0):.1f}",
                            "avg_util":f"{ss.get('avg_util',0):.1f}",
                            "max_mem_mb":f"{ss.get('max_mem',0):.0f}",
                            "avg_cpu_w":f"{ss.get('avg_cpu_w',0):.1f}",
                            "gpu_energy_j":f"{gpu_e:.4f}","cpu_energy_j":f"{cpu_e:.4f}",
                            "total_energy_j":f"{gpu_e+cpu_e:.4f}","status":r["status"]})

                        if r["status"]=="OK":
                            print(f"    {q}({n_reps}r): {qt_s*1000:.1f}ms P={ss.get('avg_pw',0):.0f}W E={gpu_e:.2f}J")
                        else:
                            print(f"    {q}: {r['status']}")
                    sys.stdout.flush()

            # ── Validate metrics ──────────────────────────────────────
            ok_m = sum(1 for r in m_rows if r["status"]=="OK")
            print(f"\n  [CHECK] Metrics {tag}: {ok_m}/{len(m_rows)} OK")

            # ── Save checkpoint ───────────────────────────────────────
            t_fields = ["engine","benchmark","sf","query","storage","n_reps",
                        "run3_ms","min_ms","all_ms","status"]
            with open(checkpoint_t, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=t_fields); w.writeheader(); w.writerows(t_rows)

            m_fields = ["engine","benchmark","sf","query","storage","n_reps",
                        "min_ms","query_ms","elapsed_s","n_samples","n_steady",
                        "avg_power_w","max_power_w","avg_util","max_mem_mb","avg_cpu_w",
                        "gpu_energy_j","cpu_energy_j","total_energy_j","status"]
            with open(checkpoint_m, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=m_fields); w.writeheader(); w.writerows(m_rows)

            print(f"  [SAVED] {checkpoint_t} ({len(t_rows)} rows)")
            print(f"  [SAVED] {checkpoint_m} ({len(m_rows)} rows)")
            all_timing.extend(t_rows)
            all_metrics.extend(m_rows)

    # ── Merge all checkpoints ─────────────────────────────────────────
    merged_t = results_dir / "timing_all.csv"
    merged_m = results_dir / "metrics_all.csv"
    merged_s = results_dir / "samples_all.csv"

    t_fields = ["engine","benchmark","sf","query","storage","n_reps",
                "run3_ms","min_ms","all_ms","status"]
    with open(merged_t, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=t_fields); w.writeheader(); w.writerows(all_timing)

    m_fields = ["engine","benchmark","sf","query","storage","n_reps",
                "min_ms","query_ms","elapsed_s","n_samples","n_steady",
                "avg_power_w","max_power_w","avg_util","max_mem_mb","avg_cpu_w",
                "gpu_energy_j","cpu_energy_j","total_energy_j","status"]
    with open(merged_m, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=m_fields); w.writeheader(); w.writerows(all_metrics)

    if all_samples:
        s_fields = ["run_id","t_ms","power_w","gpu_util","mem_mb","sm_mhz","cpu_w"]
        with open(merged_s, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=s_fields); w.writeheader(); w.writerows(all_samples)

    ok_total = sum(1 for r in all_timing if r["status"]=="OK")
    print(f"\n{'='*70}")
    print(f"  COMPLETE: {datetime.now()}")
    print(f"  Timing:  {merged_t} ({len(all_timing)} rows, {ok_total} OK)")
    print(f"  Metrics: {merged_m} ({len(all_metrics)} rows)")
    print(f"  Samples: {merged_s} ({len(all_samples)} samples)")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
