#!/usr/bin/env python3
"""
CPU/GPU frequency scaling experiment — TPC-H SF=5 Q1 on all engines.

Tests 3 engine modes:
  1. sirius_gpu:  Sirius (DuckDB GPU extension) — 30 reps
  2. maximus_gpu: Maximus GPU (-d gpu -s cpu) — 30 reps
  3. maximus_cpu: Maximus CPU (-d cpu -s cpu) — 10 reps

4 frequency configurations each:
  baseline:  CPU=4400MHz (performance), GPU=unlocked
  cpu_low:   CPU=800MHz  (powersave),   GPU=unlocked
  gpu_low:   CPU=4400MHz (performance), GPU=180MHz
  both_low:  CPU=800MHz  (powersave),   GPU=180MHz
"""
from __future__ import annotations

import csv
import os
import re
import subprocess
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
MAXBENCH = Path("/home/xzw/gpu_db/build/benchmarks/maxbench")
SIRIUS_DUCKDB = Path("/home/xzw/sirius/build/release/duckdb")
TPCH_CSV = Path("/home/xzw/gpu_db/tests/tpch/csv-5")
TPCH_DB = Path("/home/xzw/tpch_duckdb/tpch_sf5.duckdb")
GPU_ID = "1"
QUERY_TIMEOUT_S = 600

LD_EXTRA = [
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libkvikio/lib64",
]
_ld = os.environ.get("LD_LIBRARY_PATH", "")
os.environ["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + _ld if _ld else "")

SIRIUS_BUFFER_INIT = 'call gpu_buffer_init("10 GB", "5 GB");'
SIRIUS_GPU_QUERY = ('call gpu_processing("SELECT l_returnflag, l_linestatus, '
    'sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, '
    'sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, '
    'sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, '
    'avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, '
    'avg(l_discount) AS avg_disc, count(*) AS count_order '
    'FROM lineitem WHERE l_shipdate <= CAST(\'1998-09-02\' AS date) '
    'GROUP BY l_returnflag, l_linestatus '
    'ORDER BY l_returnflag, l_linestatus;");')

RE_RUN_TIME = re.compile(r"Run Time \(s\):\s*real\s+([\d.]+)", re.IGNORECASE)
RE_MAXIMUS_TIMES = re.compile(r"MAXIMUS TIMINGS \[ms\]:\s*(.*)")

SUDO_PASS = "xujianjun010816?"

CONFIGS = [
    {"name": "baseline",  "cpu_perf_pct": 100, "no_turbo": 0, "gpu_clk": None},
    {"name": "cpu_low",   "cpu_perf_pct": 18,  "no_turbo": 1, "gpu_clk": None},
    {"name": "gpu_low",   "cpu_perf_pct": 100, "no_turbo": 0, "gpu_clk": 180},
    {"name": "both_low",  "cpu_perf_pct": 18,  "no_turbo": 1, "gpu_clk": 180},
]

ENGINES = [
    {"name": "sirius_gpu",  "n_reps": 30},
    {"name": "maximus_gpu", "n_reps": 30},
    {"name": "maximus_cpu", "n_reps": 10},
]

# ── RAPL ──────────────────────────────────────────────────────────────────────
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


# ── Frequency control ────────────────────────────────────────────────────────
def sudo_cmd(cmd_str):
    """Run a shell command with sudo, passing password via stdin."""
    subprocess.run(
        ["sudo", "-S", "bash", "-c", cmd_str],
        input=SUDO_PASS + "\n", text=True, capture_output=True)


def set_cpu_perf(perf_pct, no_turbo):
    """Set CPU performance via intel_pstate."""
    sudo_cmd(f"echo {perf_pct} > /sys/devices/system/cpu/intel_pstate/max_perf_pct")
    sudo_cmd(f"echo {no_turbo} > /sys/devices/system/cpu/intel_pstate/no_turbo")
    time.sleep(1)
    actual_pct = int(Path("/sys/devices/system/cpu/intel_pstate/max_perf_pct").read_text().strip())
    actual_turbo = int(Path("/sys/devices/system/cpu/intel_pstate/no_turbo").read_text().strip())
    actual_freq = int(Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq").read_text().strip())
    print(f"  [CPU] max_perf_pct={actual_pct}%, no_turbo={actual_turbo}, cur_freq={actual_freq//1000}MHz")


def set_gpu_clk(clk_mhz):
    if clk_mhz is not None:
        sudo_cmd(f"nvidia-smi -i {GPU_ID} -lgc {clk_mhz},{clk_mhz}")
        print(f"  [GPU] Locked SM clock to {clk_mhz}MHz")
    else:
        sudo_cmd(f"nvidia-smi -i {GPU_ID} -rgc")
        print(f"  [GPU] Unlocked SM clock")


def restore_defaults():
    set_cpu_perf(100, 0)
    sudo_cmd(f"nvidia-smi -i {GPU_ID} -rgc")
    print("  [RESTORED] defaults")


def apply_config(cfg):
    set_cpu_perf(cfg["cpu_perf_pct"], cfg["no_turbo"])
    set_gpu_clk(cfg["gpu_clk"])
    time.sleep(2)


# ── Power sampling ────────────────────────────────────────────────────────────
def sample_metrics(stop_event, samples, interval=0.05):
    start = time.time()
    prev_pkg, prev_dram = read_rapl_uj()
    prev_time = start
    while not stop_event.is_set():
        try:
            r = subprocess.run(
                ["nvidia-smi", "-i", GPU_ID,
                 "--query-gpu=power.draw,utilization.gpu,memory.used,clocks.current.sm",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5)
            now = time.time()
            cur_pkg, cur_dram = read_rapl_uj()
            dt = now - prev_time
            cpu_pkg_w = (cur_pkg - prev_pkg) / 1e6 / dt if dt > 0 else 0
            cpu_dram_w = (cur_dram - prev_dram) / 1e6 / dt if dt > 0 else 0
            prev_pkg, prev_dram, prev_time = cur_pkg, cur_dram, now
            if r.returncode == 0:
                parts = [p.strip() for p in r.stdout.strip().split(",")]
                if len(parts) >= 4:
                    samples.append({
                        "time_offset_ms": int((now - start) * 1000),
                        "gpu_power_w": float(parts[0]),
                        "gpu_util_pct": float(parts[1]),
                        "mem_used_mb": float(parts[2]),
                        "sm_clk_mhz": float(parts[3]),
                        "cpu_pkg_w": round(cpu_pkg_w, 1),
                        "cpu_dram_w": round(cpu_dram_w, 1),
                    })
        except Exception:
            pass
        stop_event.wait(interval)


# ── Engine runners ────────────────────────────────────────────────────────────
def run_sirius(n_reps):
    """Run Sirius GPU, return (times_list, elapsed, status)."""
    sql_lines = [".timer on", SIRIUS_BUFFER_INIT]
    for _ in range(n_reps):
        sql_lines.append(SIRIUS_GPU_QUERY)
    sql = "\n".join(sql_lines) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql)
        tmp = f.name
    try:
        t0 = time.time()
        r = subprocess.run([str(SIRIUS_DUCKDB), str(TPCH_DB)],
                           stdin=open(tmp), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           text=True, timeout=QUERY_TIMEOUT_S)
        elapsed = time.time() - t0
        if r.returncode != 0 or "fallback" in (r.stdout or "").lower():
            return [], elapsed, "FALLBACK" if "fallback" in (r.stdout or "").lower() else "FAIL"
        times = [float(m.group(1)) for m in RE_RUN_TIME.finditer(r.stdout)]
        cached = times[2:] if len(times) > 2 else times[1:] if len(times) > 1 else times
        return cached, elapsed, "OK"
    except subprocess.TimeoutExpired:
        return [], time.time() - t0, "TIMEOUT"
    finally:
        os.unlink(tmp)


def run_maximus(n_reps, device="gpu"):
    """Run Maximus, return (times_list, elapsed, status)."""
    cmd = [str(MAXBENCH), "--benchmark", "tpch", "-q", "q1",
           "-d", device, "-r", str(n_reps),
           "--path", str(TPCH_CSV), "-s", "cpu", "--engines", "maximus"]
    try:
        t0 = time.time()
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=QUERY_TIMEOUT_S)
        elapsed = time.time() - t0
        if r.returncode != 0:
            print(f"    [ERROR] rc={r.returncode}")
            print(f"    stderr: {(r.stderr or '')[:200]}")
            return [], elapsed, "FAIL"
        # Parse "MAXIMUS TIMINGS [ms]: 	175, 	143, ..."
        m = RE_MAXIMUS_TIMES.search(r.stdout)
        if m:
            raw = m.group(1).strip().rstrip(",")
            times_ms = [float(t.strip()) for t in raw.split(",") if t.strip()]
            times_s = [t / 1000.0 for t in times_ms]
            # Skip first rep (cold)
            cached = times_s[1:] if len(times_s) > 1 else times_s
            return cached, elapsed, "OK"
        return [], elapsed, "PARSE_ERROR"
    except subprocess.TimeoutExpired:
        return [], time.time() - t0, "TIMEOUT"


def compute_steady_state(samples):
    if not samples:
        return samples
    all_util = [s["gpu_util_pct"] for s in samples]
    avg_util = sum(all_util) / len(all_util)
    start_idx = next((i for i, s in enumerate(samples) if s["gpu_util_pct"] >= avg_util), 0)
    end_idx = next((i for i in range(len(samples)-1, -1, -1) if samples[i]["gpu_util_pct"] >= avg_util), len(samples)-1)
    return samples[start_idx:end_idx+1] if end_idx >= start_idx else samples


def run_with_sampling(engine_name, n_reps, device="gpu"):
    """Run engine with power sampling, return result dict."""
    samples = []
    stop_event = threading.Event()
    sampler = threading.Thread(target=sample_metrics, args=(stop_event, samples, 0.05))
    sampler.start()

    if engine_name == "sirius_gpu":
        times, elapsed, status = run_sirius(n_reps)
    elif engine_name == "maximus_gpu":
        times, elapsed, status = run_maximus(n_reps, "gpu")
    else:
        times, elapsed, status = run_maximus(n_reps, "cpu")

    stop_event.set()
    sampler.join(timeout=5)

    steady = compute_steady_state(samples)

    if steady:
        avg_gpu_w = sum(s["gpu_power_w"] for s in steady) / len(steady)
        max_gpu_w = max(s["gpu_power_w"] for s in steady)
        avg_util = sum(s["gpu_util_pct"] for s in steady) / len(steady)
        avg_clk = sum(s["sm_clk_mhz"] for s in steady) / len(steady)
        avg_cpu_w = sum(s["cpu_pkg_w"] for s in steady) / len(steady)
        avg_dram_w = sum(s["cpu_dram_w"] for s in steady) / len(steady)
        max_mem = max(s["mem_used_mb"] for s in steady)
    else:
        avg_gpu_w = max_gpu_w = avg_util = avg_clk = avg_cpu_w = avg_dram_w = max_mem = 0

    min_s = min(times) if times else 0
    avg_s = sum(times) / len(times) if times else 0

    return {
        "status": status, "times": times,
        "min_s": min_s, "avg_s": avg_s,
        "n_cached": len(times), "elapsed_s": elapsed,
        "num_samples": len(samples), "num_steady": len(steady),
        "avg_gpu_w": avg_gpu_w, "max_gpu_w": max_gpu_w,
        "avg_util": avg_util, "avg_clk": avg_clk,
        "avg_cpu_w": avg_cpu_w, "avg_dram_w": avg_dram_w,
        "max_mem_mb": max_mem,
        "gpu_energy_j": avg_gpu_w * min_s,
        "cpu_energy_j": avg_cpu_w * min_s,
        "samples": samples,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    results_dir = Path("/home/xzw/gpu_db/results/freq_experiment")
    results_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 70)
    print("  FREQUENCY SCALING EXPERIMENT — ALL ENGINES")
    print("  TPC-H SF=5 Q1")
    print(f"  Engines: {', '.join(e['name'] + '(' + str(e['n_reps']) + ' reps)' for e in ENGINES)}")
    print(f"  Started: {datetime.now()}")
    print("=" * 70)

    all_results = []
    all_samples = []

    for cfg in CONFIGS:
        cname = cfg["name"]
        cpu_pct = cfg["cpu_perf_pct"]
        gpu_mhz = cfg["gpu_clk"] if cfg["gpu_clk"] else "auto"

        print(f"\n{'='*70}")
        print(f"  CONFIG: {cname} (CPU={cpu_pct}%, GPU={gpu_mhz}MHz)")
        print(f"{'='*70}")
        apply_config(cfg)

        for eng in ENGINES:
            ename = eng["name"]
            n_reps = eng["n_reps"]
            print(f"\n  --- {ename} ({n_reps} reps) ---")

            result = run_with_sampling(ename, n_reps)

            if result["status"] == "OK":
                print(f"    min={result['min_s']:.4f}s, avg={result['avg_s']:.4f}s, "
                      f"elapsed={result['elapsed_s']:.1f}s, n={result['n_cached']}")
                print(f"    GPU: {result['avg_gpu_w']:.1f}W (max {result['max_gpu_w']:.1f}W), "
                      f"util={result['avg_util']:.0f}%, clk={result['avg_clk']:.0f}MHz")
                print(f"    CPU: {result['avg_cpu_w']:.1f}W, DRAM: {result['avg_dram_w']:.1f}W")
                print(f"    Energy: GPU={result['gpu_energy_j']:.2f}J, CPU={result['cpu_energy_j']:.2f}J")
            else:
                print(f"    STATUS: {result['status']}")

            row = {
                "config": cname, "engine": ename,
                "cpu_perf_pct": cpu_pct, "gpu_clk_mhz": gpu_mhz,
                "n_reps": n_reps, "status": result["status"],
                "min_s": f"{result['min_s']:.4f}" if result["status"] == "OK" else "",
                "avg_s": f"{result['avg_s']:.4f}" if result["status"] == "OK" else "",
                "elapsed_s": f"{result['elapsed_s']:.1f}",
                "avg_gpu_w": f"{result['avg_gpu_w']:.1f}" if result["status"] == "OK" else "",
                "max_gpu_w": f"{result['max_gpu_w']:.1f}" if result["status"] == "OK" else "",
                "avg_util": f"{result['avg_util']:.0f}" if result["status"] == "OK" else "",
                "avg_clk": f"{result['avg_clk']:.0f}" if result["status"] == "OK" else "",
                "avg_cpu_w": f"{result['avg_cpu_w']:.1f}" if result["status"] == "OK" else "",
                "avg_dram_w": f"{result['avg_dram_w']:.1f}" if result["status"] == "OK" else "",
                "gpu_energy_j": f"{result['gpu_energy_j']:.2f}" if result["status"] == "OK" else "",
                "cpu_energy_j": f"{result['cpu_energy_j']:.2f}" if result["status"] == "OK" else "",
                "max_mem_mb": f"{result['max_mem_mb']:.0f}" if result["status"] == "OK" else "",
            }
            all_results.append(row)

            for s in result.get("samples", []):
                s["config"] = cname
                s["engine"] = ename
            all_samples.extend(result.get("samples", []))

        print(f"\n  Cooling 5s...")
        time.sleep(5)

    restore_defaults()

    # Save summary
    summary_file = results_dir / f"freq_all_summary_{ts}.csv"
    fields = ["config", "engine", "cpu_perf_pct", "gpu_clk_mhz", "n_reps", "status",
              "min_s", "avg_s", "elapsed_s",
              "avg_gpu_w", "max_gpu_w", "avg_util", "avg_clk",
              "avg_cpu_w", "avg_dram_w", "gpu_energy_j", "cpu_energy_j", "max_mem_mb"]
    with open(summary_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(all_results)

    # Save samples
    samples_file = results_dir / f"freq_all_samples_{ts}.csv"
    sample_fields = ["config", "engine", "time_offset_ms", "gpu_power_w", "gpu_util_pct",
                     "mem_used_mb", "sm_clk_mhz", "cpu_pkg_w", "cpu_dram_w"]
    with open(samples_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=sample_fields)
        w.writeheader()
        w.writerows([{k: s.get(k, "") for k in sample_fields} for s in all_samples])

    print(f"\n{'='*70}")
    print(f"  EXPERIMENT COMPLETE")
    print(f"  Summary: {summary_file}")
    print(f"  Samples: {samples_file} ({len(all_samples)} samples)")
    print(f"  Finished: {datetime.now()}")
    print(f"{'='*70}")

    # Print comparison table
    print(f"\n  {'Config':<10} {'Engine':<14} {'Min(s)':>8} {'Avg(s)':>8} "
          f"{'GPU_W':>6} {'CPU_W':>6} {'GPU_E':>7} {'CPU_E':>7} {'Status':<8}")
    print(f"  {'-'*10} {'-'*14} {'-'*8} {'-'*8} {'-'*6} {'-'*6} {'-'*7} {'-'*7} {'-'*8}")
    for r in all_results:
        if r["status"] == "OK":
            print(f"  {r['config']:<10} {r['engine']:<14} {r['min_s']:>8} {r['avg_s']:>8} "
                  f"{r['avg_gpu_w']:>6} {r['avg_cpu_w']:>6} {r['gpu_energy_j']:>7} {r['cpu_energy_j']:>7} {r['status']:<8}")
        else:
            print(f"  {r['config']:<10} {r['engine']:<14} {'':>8} {'':>8} "
                  f"{'':>6} {'':>6} {'':>7} {'':>7} {r['status']:<8}")


if __name__ == "__main__":
    main()
