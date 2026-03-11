#!/usr/bin/env python3
"""
Add Maximus -s cpu (data on CPU) runs to the existing freq sweep.
Runs only the missing maximus_cpu/ data for each config.
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
GPU_ID = "1"
SUDO_PASS = "xujianjun010816?"

CONFIGS = [
    {"name": "baseline",  "cpu_perf_pct": 100, "no_turbo": 0, "gpu_clk": None},
    {"name": "cpu_low",   "cpu_perf_pct": 18,  "no_turbo": 1, "gpu_clk": None},
    {"name": "gpu_low",   "cpu_perf_pct": 100, "no_turbo": 0, "gpu_clk": 180},
    {"name": "both_low",  "cpu_perf_pct": 18,  "no_turbo": 1, "gpu_clk": 180},
]

BENCHMARKS = ["tpch", "h2o", "clickbench"]
TARGET_TIME = 10


def sudo_cmd(cmd_str):
    subprocess.run(
        ["sudo", "-S", "bash", "-c", cmd_str],
        input=SUDO_PASS + "\n", text=True, capture_output=True)


def set_freq_config(cfg):
    pct = cfg["cpu_perf_pct"]
    nt = cfg["no_turbo"]
    sudo_cmd(f"echo {pct} > /sys/devices/system/cpu/intel_pstate/max_perf_pct")
    sudo_cmd(f"echo {nt} > /sys/devices/system/cpu/intel_pstate/no_turbo")
    if cfg["gpu_clk"] is not None:
        sudo_cmd(f"nvidia-smi -i {GPU_ID} -lgc {cfg['gpu_clk']},{cfg['gpu_clk']}")
    else:
        sudo_cmd(f"nvidia-smi -i {GPU_ID} -rgc")
    time.sleep(2)
    actual_pct = Path("/sys/devices/system/cpu/intel_pstate/max_perf_pct").read_text().strip()
    actual_nt = Path("/sys/devices/system/cpu/intel_pstate/no_turbo").read_text().strip()
    actual_freq = int(Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq").read_text().strip()) // 1000
    gpu_clk_str = f"{cfg['gpu_clk']}MHz" if cfg['gpu_clk'] else "auto"
    print(f"  [FREQ] CPU: max_perf={actual_pct}%, no_turbo={actual_nt}, cur={actual_freq}MHz | GPU: {gpu_clk_str}")


def restore_defaults():
    sudo_cmd("echo 100 > /sys/devices/system/cpu/intel_pstate/max_perf_pct")
    sudo_cmd("echo 0 > /sys/devices/system/cpu/intel_pstate/no_turbo")
    sudo_cmd(f"nvidia-smi -i {GPU_ID} -rgc")
    print("  [FREQ] Restored defaults")


def main():
    results_base = Path("/home/xzw/gpu_db/results/freq_sweep")
    total_start = time.time()

    print(f"\n{'='*70}")
    print(f"  FREQ SWEEP — ADD MAXIMUS -s cpu RUNS")
    print(f"  Started: {datetime.now()}")
    print(f"{'='*70}\n")

    for idx, cfg in enumerate(CONFIGS):
        cname = cfg["name"]
        cfg_dir = results_base / cname
        cpu_dir = cfg_dir / "maximus_cpu"

        # Skip if already done
        if cpu_dir.exists() and list(cpu_dir.glob("*_metrics_summary_*.csv")):
            print(f"  SKIP {cname}: maximus_cpu/ already has results")
            continue

        cpu_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n{'='*70}")
        print(f"  CONFIG {idx+1}/4: {cname} — Maximus -s cpu")
        print(f"{'='*70}")

        set_freq_config(cfg)

        cmd = [
            sys.executable, str(SCRIPT_DIR / "run_maximus_metrics.py"),
            *BENCHMARKS,
            "--results-dir", str(cpu_dir),
            "--target-time", str(TARGET_TIME),
            "--storage", "cpu",
        ]
        print(f"  [RUN] {' '.join(cmd)}")
        t0 = time.time()
        try:
            r = subprocess.run(cmd, timeout=7200)
            elapsed = time.time() - t0
            print(f"  Maximus -s cpu: {'OK' if r.returncode == 0 else 'FAIL'} in {elapsed/60:.1f}min")
        except subprocess.TimeoutExpired:
            print(f"  Maximus -s cpu: TIMEOUT after {(time.time()-t0)/60:.1f}min")

        print(f"  Cooling 10s...")
        time.sleep(10)

    restore_defaults()

    total_elapsed = time.time() - total_start
    print(f"\n{'='*70}")
    print(f"  COMPLETE — {timedelta(seconds=int(total_elapsed))}")
    print(f"  Finished: {datetime.now()}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
