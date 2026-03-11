#!/usr/bin/env python3
"""
Fix freq_sweep: re-run Sirius for gpu_low and both_low configs,
and Maximus clickbench for gpu_low.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
MAXIMUS_DIR = SCRIPT_DIR.parent.parent
GPU_ID = "1"
RESULTS_BASE = MAXIMUS_DIR / "results" / "freq_sweep"
TARGET_TIME = 10

CONFIGS_TO_FIX = [
    {"name": "gpu_low",   "cpu_perf_pct": 100, "no_turbo": 0, "gpu_clk": 180},
    {"name": "both_low",  "cpu_perf_pct": 18,  "no_turbo": 1, "gpu_clk": 180},
]


def sudo_cmd(cmd_str):
    subprocess.run(
        ["sudo", "bash", "-c", cmd_str],
        text=True, capture_output=True)


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
    gpu_clk_str = f"{cfg['gpu_clk']}MHz" if cfg['gpu_clk'] else "auto"
    print(f"  [FREQ] CPU: max_perf={actual_pct}% | GPU: {gpu_clk_str}")


def restore_defaults():
    sudo_cmd("echo 100 > /sys/devices/system/cpu/intel_pstate/max_perf_pct")
    sudo_cmd("echo 0 > /sys/devices/system/cpu/intel_pstate/no_turbo")
    sudo_cmd(f"nvidia-smi -i {GPU_ID} -rgc")
    print("  [FREQ] Restored defaults")


def run_metrics(script_name, benchmarks, results_dir, extra_args=None):
    cmd = [
        sys.executable, str(SCRIPT_DIR / script_name),
        *benchmarks,
        "--results-dir", str(results_dir),
        "--target-time", str(TARGET_TIME),
    ]
    if extra_args:
        cmd.extend(extra_args)
    print(f"  [RUN] {' '.join(cmd)}")
    t0 = time.time()
    try:
        r = subprocess.run(cmd, timeout=7200)
        elapsed = time.time() - t0
        return r.returncode == 0, elapsed
    except subprocess.TimeoutExpired:
        return False, time.time() - t0


def main():
    total_start = time.time()
    print(f"\n{'='*70}")
    print(f"  FREQ SWEEP FIX — re-run failed configs")
    print(f"  Started: {datetime.now()}")
    print(f"{'='*70}\n")

    for cfg in CONFIGS_TO_FIX:
        cname = cfg["name"]
        cfg_dir = RESULTS_BASE / cname

        print(f"\n{'='*70}")
        print(f"  CONFIG: {cname}")
        print(f"{'='*70}")

        set_freq_config(cfg)

        # Re-run Sirius (all benchmarks)
        print(f"\n  --- Sirius metrics for {cname} ---")
        ok, elapsed = run_metrics("run_sirius_metrics.py",
                                  ["tpch", "h2o", "clickbench"],
                                  str(cfg_dir))
        print(f"  Sirius: {'OK' if ok else 'FAIL'} in {elapsed/60:.1f}min")

        # For gpu_low, also run Maximus clickbench (was missing)
        if cname == "gpu_low":
            print(f"\n  --- Maximus clickbench for {cname} ---")
            ok, elapsed = run_metrics("run_maximus_metrics.py",
                                      ["clickbench"],
                                      str(cfg_dir),
                                      ["--storage", "gpu"])
            print(f"  Maximus clickbench: {'OK' if ok else 'FAIL'} in {elapsed/60:.1f}min")

        print(f"  Cooling 10s...")
        time.sleep(10)

    restore_defaults()

    total_elapsed = time.time() - total_start
    print(f"\n{'='*70}")
    print(f"  COMPLETE — {timedelta(seconds=int(total_elapsed))}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
