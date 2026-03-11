# GPU Energy Sweep Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create `benchmarks/scripts/run_energy_sweep.py` that sweeps GPU power limit and SM clock frequency across all successful benchmarks to find the most energy-efficient configuration.

**Architecture:** Single orchestration script that sets GPU config via `sudo nvidia-smi`, then calls existing `run_maximus_metrics.py` and `run_sirius_metrics.py` as subprocesses for each config point. Results are saved per-config and aggregated into a summary CSV.

**Tech Stack:** Python 3.12, subprocess (nvidia-smi + existing metrics scripts), csv, argparse

---

## Context: Existing Code

- `benchmarks/scripts/run_maximus_metrics.py` — Maximus metrics runner. Takes `benchmarks [--sf SF] [--target-time T] [--results-dir DIR]`. Outputs `maximus_{benchmark}_sf{sf}_metrics_summary_{ts}.csv` and `..._samples_{ts}.csv`.
- `benchmarks/scripts/run_sirius_metrics.py` — Sirius metrics runner. Takes `benchmarks [--target-time T] [--results-dir DIR]`. Outputs `sirius_{benchmark}_sf{sf}_metrics_summary_{ts}.csv` and `..._samples_{ts}.csv`.
- Both scripts handle OOM gracefully (skip with status).
- GPU_ID = "1" (RTX 5080) is hardcoded in both.

## Parameter Grid

| Dimension | Values | Count |
|-----------|--------|-------|
| Power Limit (W) | 250, 275, 300, 325, 360, 450 | 6 |
| SM Clock (MHz) | 600, 1200, 1800, 2400, 3090 | 5 |
| **Total** | | **30** |

## Benchmark Matrix

| Engine | Benchmark | Scale Factors |
|--------|-----------|---------------|
| Maximus | TPC-H | 1, 2 |
| Maximus | H2O | 1gb, 2gb |
| Maximus | ClickBench | 5 |
| Sirius | TPC-H | 1, 2, 5, 10 |
| Sirius | H2O | 1gb, 2gb, 3gb, 4gb |
| Sirius | ClickBench | 10, 20 |

---

### Task 1: Create the energy sweep script with GPU config management

**Files:**
- Create: `benchmarks/scripts/run_energy_sweep.py`

**Step 1: Write the script skeleton with GPU config functions**

```python
#!/usr/bin/env python3
"""
GPU Energy Sweep: find most energy-efficient GPU configuration.

Sweeps power limit × SM clock frequency across all benchmarks,
measuring power/energy at each configuration point.

Usage:
    python run_energy_sweep.py
    python run_energy_sweep.py --power-limits 250,300,360 --sm-clocks 600,1800,3090
    python run_energy_sweep.py --engines maximus --benchmarks tpch
    python run_energy_sweep.py --resume  # skip already-completed configs

Output:
    results/energy_sweep/pl{PL}w_clk{CLK}mhz/  — per-config metrics CSVs
    results/energy_sweep/energy_sweep_summary.csv — aggregated results
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent.parent
GPU_ID = "1"

DEFAULT_POWER_LIMITS = [250, 275, 300, 325, 360, 450]
DEFAULT_SM_CLOCKS = [600, 1200, 1800, 2400, 3090]
DEFAULT_POWER_LIMIT = 360  # RTX 5080 default

# Maximus benchmark configs: (benchmark, scale_factors)
MAXIMUS_BENCHMARKS = {
    "tpch": [1, 2],
    "h2o": ["1gb", "2gb"],
    "clickbench": [5],
}

# Sirius benchmark configs
SIRIUS_BENCHMARKS = {
    "tpch": [1, 2, 5, 10],
    "h2o": ["1gb", "2gb", "3gb", "4gb"],
    "clickbench": [10, 20],
}


def run_cmd(cmd: list[str], check=True, timeout=30) -> subprocess.CompletedProcess:
    """Run a shell command, return result."""
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def set_gpu_config(power_limit_w: int, sm_clock_mhz: int) -> bool:
    """Set GPU power limit and lock SM clock. Returns True on success."""
    try:
        r1 = run_cmd(["sudo", "nvidia-smi", "-i", GPU_ID, "-pl", str(power_limit_w)])
        if r1.returncode != 0:
            print(f"  ERROR setting power limit: {r1.stderr.strip()}")
            return False

        r2 = run_cmd(["sudo", "nvidia-smi", "-i", GPU_ID,
                       f"--lock-gpu-clocks={sm_clock_mhz},{sm_clock_mhz}"])
        if r2.returncode != 0:
            print(f"  ERROR locking clocks: {r2.stderr.strip()}")
            return False

        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def restore_gpu_defaults():
    """Restore GPU to default settings."""
    print("\n--- Restoring GPU defaults ---")
    run_cmd(["sudo", "nvidia-smi", "-i", GPU_ID, "-pl", str(DEFAULT_POWER_LIMIT)])
    run_cmd(["sudo", "nvidia-smi", "-i", GPU_ID, "--reset-gpu-clocks"])
    print("  Power limit: 360W, clocks: unlocked")


def verify_gpu_config(power_limit_w: int, sm_clock_mhz: int) -> bool:
    """Read back GPU settings and verify they match."""
    r = run_cmd(["nvidia-smi", "-i", GPU_ID,
                 "--query-gpu=power.limit,clocks.current.sm",
                 "--format=csv,noheader,nounits"])
    if r.returncode != 0:
        return False
    parts = [p.strip() for p in r.stdout.strip().split(",")]
    if len(parts) >= 2:
        actual_pl = float(parts[0])
        actual_clk = int(float(parts[1]))
        ok = abs(actual_pl - power_limit_w) < 5
        # Clock might not be exactly what we set if GPU is idle (P8 state),
        # so we just check the lock was accepted (set_gpu_config returned True)
        print(f"  Verified: PL={actual_pl:.0f}W, SM={actual_clk}MHz")
        return ok
    return False


def get_gpu_temperature() -> float:
    """Read current GPU temperature in Celsius."""
    r = run_cmd(["nvidia-smi", "-i", GPU_ID,
                 "--query-gpu=temperature.gpu", "--format=csv,noheader,nounits"])
    if r.returncode == 0:
        return float(r.stdout.strip())
    return 0.0


def wait_for_cooldown(max_temp=85, poll_interval=10):
    """Wait until GPU temperature drops below threshold."""
    temp = get_gpu_temperature()
    if temp >= max_temp:
        print(f"  GPU temperature {temp}°C >= {max_temp}°C, waiting for cooldown...")
        while temp >= max_temp:
            time.sleep(poll_interval)
            temp = get_gpu_temperature()
            print(f"    {temp}°C...", flush=True)
        print(f"  Cooled to {temp}°C, resuming.")


def enable_persistence_mode():
    """Enable persistence mode for stable GPU settings."""
    r = run_cmd(["sudo", "nvidia-smi", "-i", GPU_ID, "-pm", "1"])
    if r.returncode == 0:
        print("  Persistence mode enabled")
    else:
        print(f"  WARNING: Could not enable persistence mode: {r.stderr.strip()}")
```

**Step 2: Add benchmark orchestration**

Add the main sweep logic that calls existing metrics scripts as subprocesses:

```python
def config_tag(pl: int, clk: int) -> str:
    """Create directory/file tag for a config: pl250w_clk0600mhz"""
    return f"pl{pl}w_clk{clk:04d}mhz"


def config_dir_exists(results_base: Path, pl: int, clk: int) -> bool:
    """Check if a config has already been completed (for --resume)."""
    tag = config_tag(pl, clk)
    config_dir = results_base / tag
    if not config_dir.exists():
        return False
    # Check if at least one summary CSV exists
    return bool(list(config_dir.glob("*_metrics_summary_*.csv")))


def run_maximus_metrics(benchmarks: list[str], results_dir: Path,
                        target_time: float = 10) -> int:
    """Run Maximus metrics script for given benchmarks. Returns exit code."""
    cmd = [
        sys.executable, str(SCRIPT_DIR / "run_maximus_metrics.py"),
        *benchmarks,
        "--results-dir", str(results_dir),
        "--target-time", str(target_time),
    ]
    print(f"    CMD: {' '.join(cmd)}")
    try:
        r = subprocess.run(cmd, timeout=3600)  # 1 hour max per engine
        return r.returncode
    except subprocess.TimeoutExpired:
        print("    TIMEOUT (1 hour)")
        return -1


def run_sirius_metrics(benchmarks: list[str], results_dir: Path,
                       target_time: float = 60) -> int:
    """Run Sirius metrics script for given benchmarks. Returns exit code."""
    cmd = [
        sys.executable, str(SCRIPT_DIR / "run_sirius_metrics.py"),
        *benchmarks,
        "--results-dir", str(results_dir),
        "--target-time", str(target_time),
    ]
    print(f"    CMD: {' '.join(cmd)}")
    try:
        r = subprocess.run(cmd, timeout=7200)  # 2 hours max per engine
        return r.returncode
    except subprocess.TimeoutExpired:
        print("    TIMEOUT (2 hours)")
        return -1
```

**Step 3: Add results aggregation**

```python
def aggregate_results(results_base: Path, output_file: Path):
    """Read all per-config summary CSVs and combine into one flat CSV."""
    rows = []
    for config_dir in sorted(results_base.iterdir()):
        if not config_dir.is_dir() or not config_dir.name.startswith("pl"):
            continue
        # Parse pl and clk from directory name: pl250w_clk0600mhz
        tag = config_dir.name
        try:
            pl = int(tag.split("w_")[0].replace("pl", ""))
            clk = int(tag.split("clk")[1].replace("mhz", ""))
        except (ValueError, IndexError):
            continue

        for csv_file in sorted(config_dir.glob("*_metrics_summary_*.csv")):
            engine = "maximus" if "maximus" in csv_file.name else "sirius"
            with open(csv_file) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append({
                        "power_limit_w": pl,
                        "sm_clock_mhz": clk,
                        "engine": engine,
                        "benchmark": row.get("benchmark", ""),
                        "sf": row.get("sf", ""),
                        "query": row.get("query", ""),
                        "min_ms": row.get("min_ms", row.get("min_s", "")),
                        "avg_power_w": row.get("avg_power_w", ""),
                        "max_power_w": row.get("max_power_w", ""),
                        "energy_j": row.get("energy_j", row.get("gpu_energy_j", "")),
                        "cpu_energy_j": row.get("cpu_energy_j", ""),
                        "avg_gpu_util": row.get("avg_gpu_util", ""),
                        "status": row.get("status", ""),
                    })

    if rows:
        with open(output_file, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "power_limit_w", "sm_clock_mhz", "engine", "benchmark", "sf",
                "query", "min_ms", "avg_power_w", "max_power_w",
                "energy_j", "cpu_energy_j", "avg_gpu_util", "status",
            ])
            w.writeheader()
            w.writerows(rows)
        print(f"\nSummary: {output_file} ({len(rows)} rows)")
    return rows


def print_best_configs(rows: list[dict]):
    """Print the best (lowest energy) config for each benchmark."""
    from collections import defaultdict
    best = defaultdict(lambda: (float("inf"), "", ""))
    for r in rows:
        if r["status"] != "OK" or not r["energy_j"]:
            continue
        key = (r["engine"], r["benchmark"], r["sf"])
        e = float(r["energy_j"])
        if e < best[key][0]:
            best[key] = (e, r["power_limit_w"], r["sm_clock_mhz"])

    print("\n" + "=" * 70)
    print("  BEST CONFIGURATIONS (lowest per-query energy)")
    print("=" * 70)
    for (engine, bench, sf), (energy, pl, clk) in sorted(best.items()):
        if energy < float("inf"):
            print(f"  {engine}/{bench}/SF{sf}: PL={pl}W CLK={clk}MHz → {energy:.2f}J")
```

**Step 4: Write the main function with arg parsing and sweep loop**

```python
def main():
    parser = argparse.ArgumentParser(
        description="GPU Energy Sweep: find most energy-efficient configuration")
    parser.add_argument("--power-limits", type=str, default=None,
                        help="Comma-separated power limits in W "
                             f"(default: {DEFAULT_POWER_LIMITS})")
    parser.add_argument("--sm-clocks", type=str, default=None,
                        help="Comma-separated SM clock frequencies in MHz "
                             f"(default: {DEFAULT_SM_CLOCKS})")
    parser.add_argument("--engines", nargs="*", default=["maximus", "sirius"],
                        help="Engines to benchmark (default: maximus sirius)")
    parser.add_argument("--benchmarks", nargs="*", default=None,
                        help="Benchmarks to run (default: all configured)")
    parser.add_argument("--results-dir", type=str,
                        default=str(REPO_DIR / "results" / "energy_sweep"))
    parser.add_argument("--resume", action="store_true",
                        help="Skip already-completed configs")
    parser.add_argument("--maximus-target-time", type=float, default=10,
                        help="Target sustained time for Maximus (default: 10s)")
    parser.add_argument("--sirius-target-time", type=float, default=60,
                        help="Target sustained time for Sirius (default: 60s)")
    args = parser.parse_args()

    power_limits = ([int(x) for x in args.power_limits.split(",")]
                    if args.power_limits else DEFAULT_POWER_LIMITS)
    sm_clocks = ([int(x) for x in args.sm_clocks.split(",")]
                 if args.sm_clocks else DEFAULT_SM_CLOCKS)

    maximus_benchmarks = list(MAXIMUS_BENCHMARKS.keys())
    sirius_benchmarks = list(SIRIUS_BENCHMARKS.keys())
    if args.benchmarks:
        maximus_benchmarks = [b for b in args.benchmarks if b in MAXIMUS_BENCHMARKS]
        sirius_benchmarks = [b for b in args.benchmarks if b in SIRIUS_BENCHMARKS]

    results_base = Path(args.results_dir)
    results_base.mkdir(parents=True, exist_ok=True)

    configs = [(pl, clk) for pl in sorted(power_limits) for clk in sorted(sm_clocks)]
    total = len(configs)

    print("=" * 70)
    print("  GPU ENERGY SWEEP")
    print(f"  Configs: {total} ({len(power_limits)} PL × {len(sm_clocks)} CLK)")
    print(f"  Power limits: {power_limits} W")
    print(f"  SM clocks: {sm_clocks} MHz")
    print(f"  Engines: {args.engines}")
    print(f"  Results: {results_base}")
    print(f"  Resume: {args.resume}")
    print(f"  Started: {datetime.now()}")
    print("=" * 70)

    # Verify sudo works
    r = run_cmd(["sudo", "nvidia-smi", "-i", GPU_ID, "-pm", "1"])
    if r.returncode != 0:
        print(f"ERROR: sudo nvidia-smi failed. Need sudo access.")
        print(f"  stderr: {r.stderr.strip()}")
        sys.exit(1)
    enable_persistence_mode()

    sweep_start = time.time()
    completed = 0
    skipped = 0

    try:
        for i, (pl, clk) in enumerate(configs):
            tag = config_tag(pl, clk)
            config_results = results_base / tag

            print(f"\n{'=' * 70}")
            print(f"  CONFIG {i+1}/{total}: PL={pl}W, CLK={clk}MHz ({tag})")
            elapsed_so_far = time.time() - sweep_start
            if completed > 0:
                eta = elapsed_so_far / completed * (total - i)
                print(f"  ETA: {eta/3600:.1f}h remaining")
            print(f"{'=' * 70}")

            # Resume: skip completed configs
            if args.resume and config_dir_exists(results_base, pl, clk):
                print(f"  SKIP (already completed, --resume)")
                skipped += 1
                continue

            config_results.mkdir(parents=True, exist_ok=True)

            # Set GPU config
            if not set_gpu_config(pl, clk):
                print(f"  FAILED to set GPU config, skipping")
                continue

            verify_gpu_config(pl, clk)

            # Cool-down before measurement
            time.sleep(5)
            wait_for_cooldown()

            # Run Maximus metrics
            if "maximus" in args.engines and maximus_benchmarks:
                print(f"\n  --- Maximus: {maximus_benchmarks} ---")
                run_maximus_metrics(maximus_benchmarks, config_results,
                                   args.maximus_target_time)

            # Run Sirius metrics
            if "sirius" in args.engines and sirius_benchmarks:
                print(f"\n  --- Sirius: {sirius_benchmarks} ---")
                run_sirius_metrics(sirius_benchmarks, config_results,
                                  args.sirius_target_time)

            completed += 1

    except KeyboardInterrupt:
        print("\n\nINTERRUPTED by user")
    finally:
        restore_gpu_defaults()

    # Aggregate results
    print("\n" + "=" * 70)
    print("  AGGREGATING RESULTS")
    print("=" * 70)

    summary_file = results_base / "energy_sweep_summary.csv"
    rows = aggregate_results(results_base, summary_file)
    if rows:
        print_best_configs(rows)

    total_time = time.time() - sweep_start
    print(f"\n{'=' * 70}")
    print(f"  SWEEP COMPLETE")
    print(f"  Configs: {completed} completed, {skipped} skipped, "
          f"{total - completed - skipped} failed")
    print(f"  Total time: {total_time/3600:.1f} hours")
    print(f"  Summary: {summary_file}")
    print(f"  Finished: {datetime.now()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
```

**Step 5: Make executable and verify syntax**

Run: `chmod +x benchmarks/scripts/run_energy_sweep.py && python -c "import py_compile; py_compile.compile('benchmarks/scripts/run_energy_sweep.py', doraise=True)"`
Expected: No errors

**Step 6: Smoke test with --help**

Run: `python benchmarks/scripts/run_energy_sweep.py --help`
Expected: Shows usage with all options

**Step 7: Commit**

```bash
git add benchmarks/scripts/run_energy_sweep.py
git commit -m "feat: add GPU energy sweep script for power/frequency optimization"
```

---

### Task 2: End-to-end test with minimal config

This task validates the full pipeline with a single (pl, clk) config and one fast benchmark.

**Step 1: Run a minimal sweep (1 config, TPC-H only, Maximus only)**

Run:
```bash
python benchmarks/scripts/run_energy_sweep.py \
    --power-limits 360 --sm-clocks 3090 \
    --engines maximus --benchmarks tpch \
    --results-dir results/energy_sweep_test
```
Expected: Completes without errors, creates `results/energy_sweep_test/pl360w_clk3090mhz/` with summary CSVs

**Step 2: Verify output files**

Run: `ls -la results/energy_sweep_test/pl360w_clk3090mhz/`
Expected: `maximus_tpch_sf*_metrics_summary_*.csv` files

Run: `cat results/energy_sweep_test/energy_sweep_summary.csv | head -5`
Expected: CSV with columns: power_limit_w, sm_clock_mhz, engine, benchmark, ...

**Step 3: Test --resume flag**

Run same command with `--resume` added.
Expected: Prints "SKIP (already completed, --resume)" and finishes quickly.

**Step 4: Test GPU restore on Ctrl+C**

Run a sweep and press Ctrl+C mid-execution, then verify:
```bash
nvidia-smi -i 1 --query-gpu=power.limit --format=csv,noheader,nounits
```
Expected: Should show `360.00` (restored to default)

**Step 5: Clean up test results**

Run: `rm -rf results/energy_sweep_test`

---

### Task 3: Run the full energy sweep

**Step 1: Start the full sweep**

Run:
```bash
nohup python benchmarks/scripts/run_energy_sweep.py \
    --results-dir results/energy_sweep \
    > results/energy_sweep/sweep.log 2>&1 &
echo $! > results/energy_sweep/sweep.pid
```
Expected: Background process starts, PID saved

**Step 2: Monitor progress**

Run: `tail -f results/energy_sweep/sweep.log`
Expected: Ongoing progress output showing config X/30, ETA

**Step 3: After completion, review summary**

Run: `cat results/energy_sweep/energy_sweep_summary.csv | head -20`
Expected: Full summary CSV with all config × benchmark combinations

---

## Key Design Decisions

1. **Subprocess delegation** — Calls existing metrics scripts as subprocesses rather than importing their code. This avoids refactoring, keeps scripts independently testable, and isolates failures.

2. **Per-config directories** — Each (pl, clk) config gets its own directory under `results/energy_sweep/`. This enables `--resume` and makes results easy to browse.

3. **Unified summary CSV** — Aggregates all per-config results into one flat CSV for easy analysis in pandas/Excel. Normalizes column names between Maximus (`min_ms`, `energy_j`) and Sirius (`min_s`, `gpu_energy_j`).

4. **Safety-first** — `try/finally` ensures GPU defaults are always restored, even on Ctrl+C or crashes. Temperature check prevents thermal throttling from corrupting results.
