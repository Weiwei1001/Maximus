#!/usr/bin/env python3
"""
CPU/GPU frequency scaling experiment — Sirius TPC-H SF=5 Q1.

4 configurations:
  1. baseline:  CPU=4400MHz (performance), GPU=unlocked
  2. cpu_low:   CPU=800MHz  (powersave),   GPU=unlocked
  3. gpu_low:   CPU=4400MHz (performance), GPU=180MHz
  4. both_low:  CPU=800MHz  (powersave),   GPU=180MHz

Each config runs:
  - Timing: 3 passes × 30 reps (reports last pass, min time)
  - Metrics: 30 reps with nvidia-smi + RAPL power sampling at 50ms
"""
from __future__ import annotations

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

# ── Config ────────────────────────────────────────────────────────────────────
SIRIUS_DIR = Path("/home/xzw/sirius")
DUCKDB_BIN = SIRIUS_DIR / "build" / "release" / "duckdb"
DB_PATH = Path("/home/xzw/tpch_duckdb/tpch_sf5.duckdb")
GPU_ID = "1"
N_REPS = 30
N_TIMING_PASSES = 3
QUERY_TIMEOUT_S = 300

BUFFER_INIT = 'call gpu_buffer_init("10 GB", "5 GB");'
GPU_QUERY = ('call gpu_processing("SELECT l_returnflag, l_linestatus, '
             'sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, '
             'sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, '
             'sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, '
             'avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, '
             'avg(l_discount) AS avg_disc, count(*) AS count_order '
             'FROM lineitem WHERE l_shipdate <= CAST(\'1998-09-02\' AS date) '
             'GROUP BY l_returnflag, l_linestatus '
             'ORDER BY l_returnflag, l_linestatus;");')

LD_EXTRA = [
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/nvidia/libnvcomp/lib64",
    "/home/xzw/Maximus/.venv/lib/python3.12/site-packages/libkvikio/lib64",
]
_ld = os.environ.get("LD_LIBRARY_PATH", "")
os.environ["LD_LIBRARY_PATH"] = ":".join(LD_EXTRA) + (":" + _ld if _ld else "")

RE_RUN_TIME = re.compile(r"Run Time \(s\):\s*real\s+([\d.]+)", re.IGNORECASE)

CONFIGS = [
    {"name": "baseline",  "cpu_freq": 4400000, "cpu_gov": "performance", "gpu_clk": None},
    {"name": "cpu_low",   "cpu_freq": 800000,  "cpu_gov": "powersave",   "gpu_clk": None},
    {"name": "gpu_low",   "cpu_freq": 4400000, "cpu_gov": "performance", "gpu_clk": 180},
    {"name": "both_low",  "cpu_freq": 800000,  "cpu_gov": "powersave",   "gpu_clk": 180},
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
def set_cpu_freq(freq_khz, governor):
    """Set CPU frequency for all cores."""
    n_cpus = os.cpu_count() or 1
    for i in range(n_cpus):
        subprocess.run(
            ["sudo", "tee", f"/sys/devices/system/cpu/cpu{i}/cpufreq/scaling_governor"],
            input=governor, text=True, capture_output=True)
        subprocess.run(
            ["sudo", "tee", f"/sys/devices/system/cpu/cpu{i}/cpufreq/scaling_max_freq"],
            input=str(freq_khz), text=True, capture_output=True)
        subprocess.run(
            ["sudo", "tee", f"/sys/devices/system/cpu/cpu{i}/cpufreq/scaling_min_freq"],
            input=str(freq_khz), text=True, capture_output=True)
    # Verify
    actual = int(Path(f"/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq").read_text().strip())
    print(f"  [CPU] Set governor={governor}, freq={freq_khz}kHz, actual={actual}kHz")


def set_gpu_clk(clk_mhz):
    """Lock GPU SM clock, or unlock if None."""
    if clk_mhz is not None:
        subprocess.run(
            ["sudo", "nvidia-smi", "-i", GPU_ID, "-lgc", f"{clk_mhz},{clk_mhz}"],
            capture_output=True)
        print(f"  [GPU] Locked SM clock to {clk_mhz}MHz")
    else:
        subprocess.run(
            ["sudo", "nvidia-smi", "-i", GPU_ID, "-rgc"],
            capture_output=True)
        print(f"  [GPU] Unlocked SM clock (default)")


def restore_defaults():
    """Restore CPU and GPU to defaults."""
    set_cpu_freq(4400000, "performance")
    subprocess.run(["sudo", "nvidia-smi", "-i", GPU_ID, "-rgc"], capture_output=True)
    subprocess.run(["sudo", "nvidia-smi", "-i", GPU_ID, "-rpl"], capture_output=True)
    print("  [RESTORED] CPU=performance/4400MHz, GPU=unlocked")


def apply_config(cfg):
    set_cpu_freq(cfg["cpu_freq"], cfg["cpu_gov"])
    set_gpu_clk(cfg["gpu_clk"])
    time.sleep(2)  # let settings stabilize


# ── DuckDB runner ─────────────────────────────────────────────────────────────
def run_duckdb(sql_text, timeout=QUERY_TIMEOUT_S):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql_text)
        tmp = f.name
    try:
        t0 = time.time()
        r = subprocess.run(
            [str(DUCKDB_BIN), str(DB_PATH)],
            stdin=open(tmp, "r"),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=timeout)
        elapsed = time.time() - t0
        return r.stdout or "", elapsed, r.returncode
    except subprocess.TimeoutExpired:
        return "", time.time() - t0, -1
    finally:
        os.unlink(tmp)


def parse_times(stdout):
    return [float(m.group(1)) for m in RE_RUN_TIME.finditer(stdout)]


# ── Timing test ───────────────────────────────────────────────────────────────
def run_timing(config_name):
    """Run N_TIMING_PASSES passes × N_REPS reps, report last pass min."""
    sql_lines = [".timer on", BUFFER_INIT]
    for _ in range(N_REPS):
        sql_lines.append(GPU_QUERY)
    sql = "\n".join(sql_lines) + "\n"

    all_pass_results = []
    for p in range(N_TIMING_PASSES):
        stdout, elapsed, rc = run_duckdb(sql)
        times = parse_times(stdout)
        has_fallback = "fallback" in stdout.lower()

        if rc == -1:
            print(f"    Pass {p+1}: TIMEOUT")
            all_pass_results.append({"pass": p+1, "status": "TIMEOUT", "times": []})
        elif has_fallback:
            print(f"    Pass {p+1}: FALLBACK")
            all_pass_results.append({"pass": p+1, "status": "FALLBACK", "times": []})
        else:
            # Skip buffer_init time (times[0]) and first query (times[1] = cold)
            cached = times[2:] if len(times) > 2 else times[1:] if len(times) > 1 else times
            min_t = min(cached) if cached else 0
            avg_t = sum(cached) / len(cached) if cached else 0
            print(f"    Pass {p+1}: min={min_t:.4f}s, avg={avg_t:.4f}s, "
                  f"n={len(cached)}, elapsed={elapsed:.1f}s")
            all_pass_results.append({
                "pass": p+1, "status": "OK", "times": cached,
                "min_s": min_t, "avg_s": avg_t,
            })

    # Use last pass
    last = all_pass_results[-1] if all_pass_results else None
    return last


# ── Metrics test (power sampling) ────────────────────────────────────────────
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


def run_metrics(config_name):
    """Run N_REPS with power sampling, return summary dict."""
    sql_lines = [".timer on", BUFFER_INIT]
    for _ in range(N_REPS):
        sql_lines.append(GPU_QUERY)
    sql = "\n".join(sql_lines) + "\n"

    samples = []
    stop_event = threading.Event()
    sampler = threading.Thread(target=sample_metrics, args=(stop_event, samples, 0.05))
    sampler.start()

    stdout, elapsed, rc = run_duckdb(sql)

    stop_event.set()
    sampler.join(timeout=5)

    times = parse_times(stdout)
    has_fallback = "fallback" in stdout.lower()

    if rc == -1:
        return {"status": "TIMEOUT", "samples": samples}
    if has_fallback:
        return {"status": "FALLBACK", "samples": samples}

    cached = times[2:] if len(times) > 2 else times[1:] if len(times) > 1 else times

    # Steady-state detection via GPU utilization
    if samples:
        all_util = [s["gpu_util_pct"] for s in samples]
        avg_util = sum(all_util) / len(all_util)
        start_idx = next((i for i, s in enumerate(samples) if s["gpu_util_pct"] >= avg_util), 0)
        end_idx = next((i for i in range(len(samples)-1, -1, -1) if samples[i]["gpu_util_pct"] >= avg_util), len(samples)-1)
        steady = samples[start_idx:end_idx+1] if end_idx >= start_idx else samples
    else:
        steady = []

    if steady:
        avg_gpu_w = sum(s["gpu_power_w"] for s in steady) / len(steady)
        max_gpu_w = max(s["gpu_power_w"] for s in steady)
        avg_gpu_util = sum(s["gpu_util_pct"] for s in steady) / len(steady)
        avg_sm_clk = sum(s["sm_clk_mhz"] for s in steady) / len(steady)
        avg_cpu_w = sum(s["cpu_pkg_w"] for s in steady) / len(steady)
        avg_cpu_dram_w = sum(s["cpu_dram_w"] for s in steady) / len(steady)
        max_mem = max(s["mem_used_mb"] for s in steady)
    else:
        avg_gpu_w = max_gpu_w = avg_gpu_util = avg_sm_clk = 0
        avg_cpu_w = avg_cpu_dram_w = max_mem = 0

    min_s = min(cached) if cached else 0
    avg_s = sum(cached) / len(cached) if cached else 0

    return {
        "status": "OK",
        "min_s": min_s, "avg_s": avg_s,
        "n_cached": len(cached),
        "elapsed_s": elapsed,
        "num_samples": len(samples),
        "num_steady": len(steady),
        "avg_gpu_w": avg_gpu_w, "max_gpu_w": max_gpu_w,
        "avg_gpu_util": avg_gpu_util, "avg_sm_clk": avg_sm_clk,
        "avg_cpu_w": avg_cpu_w, "avg_cpu_dram_w": avg_cpu_dram_w,
        "max_mem_mb": max_mem,
        "gpu_energy_j": avg_gpu_w * min_s,
        "cpu_energy_j": avg_cpu_w * min_s,
        "samples": samples,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    results_dir = Path(__file__).resolve().parent.parent.parent / "results" / "freq_experiment"
    results_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 70)
    print("  FREQUENCY SCALING EXPERIMENT")
    print(f"  Sirius TPC-H SF=5 Q1, {N_REPS} reps, {N_TIMING_PASSES} timing passes")
    print(f"  Started: {datetime.now()}")
    print("=" * 70)

    all_results = []
    all_samples = []

    for cfg in CONFIGS:
        name = cfg["name"]
        cpu_mhz = cfg["cpu_freq"] // 1000
        gpu_mhz = cfg["gpu_clk"] if cfg["gpu_clk"] else "auto"

        print(f"\n{'='*70}")
        print(f"  CONFIG: {name} (CPU={cpu_mhz}MHz, GPU={gpu_mhz}MHz)")
        print(f"{'='*70}")

        apply_config(cfg)

        # Timing test
        print(f"\n  --- Timing ({N_TIMING_PASSES} passes × {N_REPS} reps) ---")
        timing = run_timing(name)

        # Metrics test
        print(f"\n  --- Metrics ({N_REPS} reps with power sampling) ---")
        metrics = run_metrics(name)

        if metrics["status"] == "OK":
            print(f"    min={metrics['min_s']:.4f}s, avg={metrics['avg_s']:.4f}s, "
                  f"elapsed={metrics['elapsed_s']:.1f}s")
            print(f"    GPU: {metrics['avg_gpu_w']:.1f}W (max {metrics['max_gpu_w']:.1f}W), "
                  f"util={metrics['avg_gpu_util']:.0f}%, clk={metrics['avg_sm_clk']:.0f}MHz")
            print(f"    CPU: {metrics['avg_cpu_w']:.1f}W (pkg), {metrics['avg_cpu_dram_w']:.1f}W (dram)")
            print(f"    Energy: GPU={metrics['gpu_energy_j']:.2f}J, CPU={metrics['cpu_energy_j']:.2f}J")
            print(f"    Samples: {metrics['num_samples']} total, {metrics['num_steady']} steady")
        else:
            print(f"    STATUS: {metrics['status']}")

        row = {
            "config": name,
            "cpu_mhz": cpu_mhz,
            "gpu_clk_mhz": gpu_mhz,
            "timing_status": timing["status"] if timing else "FAIL",
            "timing_min_s": f"{timing['min_s']:.4f}" if timing and timing["status"] == "OK" else "",
            "timing_avg_s": f"{timing['avg_s']:.4f}" if timing and timing["status"] == "OK" else "",
            "metrics_status": metrics["status"],
            "metrics_min_s": f"{metrics['min_s']:.4f}" if metrics["status"] == "OK" else "",
            "metrics_avg_s": f"{metrics['avg_s']:.4f}" if metrics["status"] == "OK" else "",
            "avg_gpu_w": f"{metrics.get('avg_gpu_w', 0):.1f}" if metrics["status"] == "OK" else "",
            "max_gpu_w": f"{metrics.get('max_gpu_w', 0):.1f}" if metrics["status"] == "OK" else "",
            "avg_gpu_util": f"{metrics.get('avg_gpu_util', 0):.0f}" if metrics["status"] == "OK" else "",
            "avg_sm_clk": f"{metrics.get('avg_sm_clk', 0):.0f}" if metrics["status"] == "OK" else "",
            "avg_cpu_w": f"{metrics.get('avg_cpu_w', 0):.1f}" if metrics["status"] == "OK" else "",
            "avg_cpu_dram_w": f"{metrics.get('avg_cpu_dram_w', 0):.1f}" if metrics["status"] == "OK" else "",
            "gpu_energy_j": f"{metrics.get('gpu_energy_j', 0):.2f}" if metrics["status"] == "OK" else "",
            "cpu_energy_j": f"{metrics.get('cpu_energy_j', 0):.2f}" if metrics["status"] == "OK" else "",
            "max_mem_mb": f"{metrics.get('max_mem_mb', 0):.0f}" if metrics["status"] == "OK" else "",
        }
        all_results.append(row)

        # Tag and collect samples
        for s in metrics.get("samples", []):
            s["config"] = name
        all_samples.extend(metrics.get("samples", []))

        # Cool down
        print(f"\n  Cooling 5s...")
        time.sleep(5)

    # Restore
    restore_defaults()

    # Save summary
    summary_file = results_dir / f"freq_experiment_summary_{ts}.csv"
    with open(summary_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "config", "cpu_mhz", "gpu_clk_mhz",
            "timing_status", "timing_min_s", "timing_avg_s",
            "metrics_status", "metrics_min_s", "metrics_avg_s",
            "avg_gpu_w", "max_gpu_w", "avg_gpu_util", "avg_sm_clk",
            "avg_cpu_w", "avg_cpu_dram_w",
            "gpu_energy_j", "cpu_energy_j", "max_mem_mb",
        ])
        w.writeheader()
        w.writerows(all_results)

    # Save samples
    samples_file = results_dir / f"freq_experiment_samples_{ts}.csv"
    with open(samples_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "config", "time_offset_ms", "gpu_power_w", "gpu_util_pct",
            "mem_used_mb", "sm_clk_mhz", "cpu_pkg_w", "cpu_dram_w",
        ])
        w.writeheader()
        w.writerows([{k: s.get(k, "") for k in w.fieldnames} for s in all_samples])

    print(f"\n{'='*70}")
    print(f"  EXPERIMENT COMPLETE")
    print(f"  Summary: {summary_file}")
    print(f"  Samples: {samples_file} ({len(all_samples)} samples)")
    print(f"  Finished: {datetime.now()}")
    print(f"{'='*70}")

    # Print comparison table
    print(f"\n  {'Config':<12} {'CPU':>6} {'GPU':>6} {'Min(s)':>8} {'Avg(s)':>8} "
          f"{'GPU_W':>6} {'CPU_W':>6} {'GPU_E(J)':>9} {'CPU_E(J)':>9}")
    print(f"  {'-'*12} {'-'*6} {'-'*6} {'-'*8} {'-'*8} {'-'*6} {'-'*6} {'-'*9} {'-'*9}")
    for r in all_results:
        if r["metrics_status"] == "OK":
            print(f"  {r['config']:<12} {r['cpu_mhz']:>6} {r['gpu_clk_mhz']:>6} "
                  f"{r['metrics_min_s']:>8} {r['metrics_avg_s']:>8} "
                  f"{r['avg_gpu_w']:>6} {r['avg_cpu_w']:>6} "
                  f"{r['gpu_energy_j']:>9} {r['cpu_energy_j']:>9}")
        else:
            print(f"  {r['config']:<12} {r['cpu_mhz']:>6} {r['gpu_clk_mhz']:>6} "
                  f"  {r['metrics_status']}")


if __name__ == "__main__":
    main()
