#!/usr/bin/env python3
"""
GPU energy sweep: explore (power-limit, SM-clock) configurations.

Iterates over a grid of GPU power limits and SM clock frequencies, running
the existing Maximus and Sirius metrics scripts at each configuration point.
Results are aggregated into a single energy_sweep_summary.csv for analysis.

Safety: GPU defaults are always restored on exit (including Ctrl+C / exceptions).

Usage:
    python run_energy_sweep.py
    python run_energy_sweep.py --power-limits 250,300,360 --sm-clocks 1200,2400
    python run_energy_sweep.py --engines maximus --benchmarks tpch h2o --resume
    python run_energy_sweep.py --results-dir results/sweep_v2 --resume
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from hw_detect import (
    detect_gpu, gpu_power_levels, gpu_sm_clock_levels,
    set_gpu_power_limit, set_gpu_sm_clock, reset_gpu_clocks,
    restore_gpu_defaults,
)

# ── Constants ─────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent

# Detected at startup in main(); set as module-level for helper functions.
GPU_ID: str = "0"
_GPU_INFO: dict = {}
DEFAULT_POWER_LIMITS: list[int] = []
DEFAULT_SM_CLOCKS: list[int] = []

MAXIMUS_BENCHMARKS = {
    "tpch": [1, 2],
    "h2o": ["1gb", "2gb"],
    "clickbench": [5],
}
SIRIUS_BENCHMARKS = {
    "tpch": [1, 2],
    "h2o": ["1gb", "2gb"],
    "clickbench": [5],
}

# Unified summary CSV columns
SUMMARY_COLUMNS = [
    "power_limit_w", "sm_clock_mhz", "engine", "benchmark", "sf",
    "query", "min_ms", "avg_power_w", "max_power_w", "energy_j",
    "cpu_energy_j", "avg_gpu_util", "status",
]

COOLDOWN_PAUSE_S = 5
MAX_COOLDOWN_TEMP_C = 85


# ══════════════════════════════════════════════════════════════════════════════
#  GPU Configuration Management
# ══════════════════════════════════════════════════════════════════════════════

def set_gpu_config(power_limit_w: int, sm_clock_mhz: int) -> bool:
    """Set GPU power limit and lock SM clocks. Returns True on success."""
    print(f"  [GPU] Setting power limit to {power_limit_w}W, SM clock to {sm_clock_mhz}MHz")

    gpu_id_int = int(GPU_ID)
    if not set_gpu_power_limit(gpu_id_int, power_limit_w):
        return False

    if not set_gpu_sm_clock(gpu_id_int, sm_clock_mhz):
        return False

    # Verify
    if not verify_gpu_config(power_limit_w, sm_clock_mhz):
        print("  [GPU] WARNING: Verification failed after setting config")
        return False

    print(f"  [GPU] Config applied and verified: PL={power_limit_w}W, CLK={sm_clock_mhz}MHz")
    return True


def _restore_gpu_defaults() -> None:
    """Restore GPU to default power limit and unlock clocks."""
    default_pl = _GPU_INFO.get("power_default_w") if _GPU_INFO else None
    print(f"\n  [GPU] Restoring defaults: PL={default_pl}W, clocks=unlocked")
    restore_gpu_defaults(int(GPU_ID), default_pl)
    print("  [GPU] Defaults restored")


def verify_gpu_config(expected_pl_w: int, expected_clk_mhz: int) -> bool:
    """Read back GPU power limit and verify it matches.

    Note: SM clock is only verifiable under load on Blackwell GPUs (RTX 50xx).
    At idle, clocks.sm reports the idle frequency regardless of --lock-gpu-clocks.
    We trust the lock command's return code for clock verification.
    """
    r = subprocess.run(
        ["nvidia-smi", "-i", GPU_ID,
         "--query-gpu=power.limit",
         "--format=csv,noheader,nounits"],
        capture_output=True, text=True, timeout=10,
    )
    if r.returncode != 0:
        return False

    try:
        actual_pl = float(r.stdout.strip())
    except (ValueError, IndexError):
        return False

    # Power limit: allow 1W tolerance (nvidia-smi may round)
    pl_ok = abs(actual_pl - expected_pl_w) <= 1.0

    if not pl_ok:
        print(f"  [GPU] PL mismatch: expected {expected_pl_w}W, got {actual_pl}W")

    return pl_ok


def get_gpu_temperature() -> float:
    """Query current GPU temperature in Celsius."""
    try:
        r = subprocess.run(
            ["nvidia-smi", "-i", GPU_ID,
             "--query-gpu=temperature.gpu",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            return float(r.stdout.strip())
    except Exception:
        pass
    return 0.0


def wait_for_cooldown(max_temp: float = MAX_COOLDOWN_TEMP_C) -> None:
    """Block until GPU temperature drops below max_temp."""
    temp = get_gpu_temperature()
    if temp <= max_temp:
        return
    print(f"  [GPU] Temperature {temp:.0f}C > {max_temp:.0f}C, waiting for cooldown...",
          end="", flush=True)
    while temp > max_temp:
        time.sleep(5)
        temp = get_gpu_temperature()
        print(f" {temp:.0f}C", end="", flush=True)
    print(" OK")


def enable_persistence_mode() -> None:
    """Enable nvidia persistence mode to reduce driver overhead."""
    r = subprocess.run(
        ["sudo", "nvidia-smi", "-i", GPU_ID, "-pm", "1"],
        capture_output=True, text=True,
    )
    if r.returncode == 0:
        print("  [GPU] Persistence mode enabled")
    else:
        print(f"  [GPU] WARNING: Could not enable persistence mode: {r.stderr.strip()}")


# ══════════════════════════════════════════════════════════════════════════════
#  Config Tag and Directory Helpers
# ══════════════════════════════════════════════════════════════════════════════

def config_tag(power_limit_w: int, sm_clock_mhz: int) -> str:
    """Generate config tag: pl250w_clk0600mhz"""
    return f"pl{power_limit_w}w_clk{sm_clock_mhz:04d}mhz"


def config_dir(results_dir: Path, power_limit_w: int, sm_clock_mhz: int) -> Path:
    """Get per-config subdirectory under results_dir."""
    return results_dir / config_tag(power_limit_w, sm_clock_mhz)


def config_has_results(cfg_dir: Path, engines: list[str]) -> bool:
    """Check if a config directory already has results for all experiment steps.

    Checks for A1 (maximus_benchmark.csv) and A3/A4 (metrics summaries).
    """
    if not cfg_dir.exists():
        return False
    if "maximus" in engines:
        if not (cfg_dir / "maximus_benchmark.csv").exists():
            return False
        if not list(cfg_dir.glob("maximus_*_metrics_summary_*.csv")):
            return False
    if "sirius" in engines:
        if not list(cfg_dir.glob("sirius_*_metrics_summary_*.csv")):
            return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  Subprocess Runners for Existing Metrics Scripts
# ══════════════════════════════════════════════════════════════════════════════

def run_maximus_timing(benchmarks: list[str], cfg_dir: Path,
                       n_reps: int = 3, test_mode: bool = False) -> int:
    """A1: Maximus GPU timing (run_maximus_benchmark.py). Returns exit code."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_maximus_benchmark.py"),
        *benchmarks,
        "--n-reps", str(n_reps),
        "--results-dir", str(cfg_dir),
    ]
    if test_mode:
        cmd.append("--test")
    print(f"  [A1 MAXIMUS TIMING] {' '.join(cmd)}")
    result = subprocess.run(cmd, timeout=7200)
    return result.returncode


def run_sirius_timing(benchmarks: list[str], cfg_dir: Path,
                      test_mode: bool = False) -> int:
    """A2: Sirius GPU timing (run_sirius_benchmark.py). Returns exit code."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_sirius_benchmark.py"),
        *benchmarks,
        "--results-dir", str(cfg_dir),
    ]
    if test_mode:
        cmd.append("--test")
    print(f"  [A2 SIRIUS TIMING] {' '.join(cmd)}")
    result = subprocess.run(cmd, timeout=7200)
    return result.returncode


def run_maximus_metrics(benchmarks: list[str], cfg_dir: Path,
                        target_time: float, timing_csv: str | None = None,
                        test_mode: bool = False) -> int:
    """A3: Maximus GPU metrics (run_maximus_metrics.py). Returns exit code."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_maximus_metrics.py"),
        *benchmarks,
        "--results-dir", str(cfg_dir),
        "--target-time", str(target_time),
    ]
    if timing_csv and os.path.exists(str(timing_csv)):
        cmd.extend(["--timing-csv", str(timing_csv)])
    if test_mode:
        cmd.append("--test")
    print(f"  [A3 MAXIMUS METRICS] {' '.join(cmd)}")
    result = subprocess.run(cmd, timeout=7200)
    return result.returncode


def run_sirius_metrics(benchmarks: list[str], cfg_dir: Path,
                       target_time: float, test_mode: bool = False) -> int:
    """A4: Sirius GPU metrics (run_sirius_metrics.py). Returns exit code."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_sirius_metrics.py"),
        *benchmarks,
        "--results-dir", str(cfg_dir),
        "--target-time", str(target_time),
    ]
    if test_mode:
        cmd.append("--test")
    print(f"  [A4 SIRIUS METRICS] {' '.join(cmd)}")
    result = subprocess.run(cmd, timeout=7200)
    return result.returncode


def run_maximus_cpu_timing(benchmarks: list[str], cfg_dir: Path,
                           test_mode: bool = False) -> int:
    """B1: Maximus CPU-data timing (run_maximus_cpu_data.py --timing-only). Returns exit code."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_maximus_cpu_data.py"),
        *benchmarks,
        "--timing-only",
        "--results-dir", str(cfg_dir),
    ]
    if test_mode:
        cmd.append("--test")
    print(f"  [B1 MAXIMUS CPU TIMING] {' '.join(cmd)}")
    result = subprocess.run(cmd, timeout=7200)
    return result.returncode


def run_maximus_cpu_metrics(benchmarks: list[str], cfg_dir: Path,
                            target_time: float, timing_csv: str | None = None,
                            test_mode: bool = False) -> int:
    """B2: Maximus CPU-data metrics (run_maximus_cpu_data.py). Returns exit code."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_maximus_cpu_data.py"),
        *benchmarks,
        "--target-time", str(target_time),
        "--results-dir", str(cfg_dir),
    ]
    if timing_csv and os.path.exists(str(timing_csv)):
        cmd.extend(["--timing-csv", str(timing_csv)])
    if test_mode:
        cmd.append("--test")
    print(f"  [B2 MAXIMUS CPU METRICS] {' '.join(cmd)}")
    result = subprocess.run(cmd, timeout=7200)
    return result.returncode


def run_sirius_cpu_data(benchmarks: list[str], cfg_dir: Path,
                        n_reps: int = 10, test_mode: bool = False) -> int:
    """B3: Sirius CPU-data timing + metrics (run_sirius_cpu_data.py). Returns exit code."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_sirius_cpu_data.py"),
        *benchmarks,
        "--n-reps", str(n_reps),
        "--results-dir", str(cfg_dir),
    ]
    if test_mode:
        cmd.append("--test")
    print(f"  [B3 SIRIUS CPU DATA] {' '.join(cmd)}")
    result = subprocess.run(cmd, timeout=7200)
    return result.returncode


# ══════════════════════════════════════════════════════════════════════════════
#  Results Aggregation
# ══════════════════════════════════════════════════════════════════════════════

def parse_maximus_summary(csv_path: Path, power_limit_w: int,
                          sm_clock_mhz: int) -> list[dict]:
    """Parse a Maximus metrics summary CSV into unified rows."""
    rows = []
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({
                    "power_limit_w": power_limit_w,
                    "sm_clock_mhz": sm_clock_mhz,
                    "engine": "maximus",
                    "benchmark": r.get("benchmark", ""),
                    "sf": r.get("sf", ""),
                    "query": r.get("query", ""),
                    "min_ms": r.get("min_ms", ""),
                    "avg_power_w": r.get("avg_power_w", ""),
                    "max_power_w": r.get("max_power_w", ""),
                    "energy_j": r.get("energy_j", ""),
                    "cpu_energy_j": r.get("cpu_energy_j", ""),
                    "avg_gpu_util": r.get("avg_gpu_util", ""),
                    "status": r.get("status", ""),
                })
    except Exception as e:
        print(f"  WARNING: Failed to parse {csv_path}: {e}")
    return rows


def parse_sirius_summary(csv_path: Path, power_limit_w: int,
                         sm_clock_mhz: int) -> list[dict]:
    """Parse a Sirius metrics summary CSV into unified rows.

    Sirius uses min_s (seconds) and gpu_energy_j; we convert to match
    the unified schema (min_ms, energy_j).
    """
    rows = []
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                # Convert min_s to min_ms
                min_s_str = r.get("min_s", "")
                try:
                    min_ms = float(min_s_str) * 1000 if min_s_str else ""
                    min_ms = f"{min_ms:.1f}" if isinstance(min_ms, float) else ""
                except (ValueError, TypeError):
                    min_ms = ""

                rows.append({
                    "power_limit_w": power_limit_w,
                    "sm_clock_mhz": sm_clock_mhz,
                    "engine": "sirius",
                    "benchmark": r.get("benchmark", ""),
                    "sf": r.get("sf", ""),
                    "query": r.get("query", ""),
                    "min_ms": min_ms,
                    "avg_power_w": r.get("avg_power_w", ""),
                    "max_power_w": r.get("max_power_w", ""),
                    "energy_j": r.get("gpu_energy_j", ""),
                    "cpu_energy_j": r.get("cpu_energy_j", ""),
                    "avg_gpu_util": r.get("avg_gpu_util", ""),
                    "status": r.get("status", ""),
                })
    except Exception as e:
        print(f"  WARNING: Failed to parse {csv_path}: {e}")
    return rows


def aggregate_results(results_dir: Path,
                      power_limits: list[int],
                      sm_clocks: list[int]) -> list[dict]:
    """Read all per-config summary CSVs and combine into unified rows."""
    all_rows = []

    for pl in power_limits:
        for clk in sm_clocks:
            cfg_d = config_dir(results_dir, pl, clk)
            if not cfg_d.exists():
                continue

            # Maximus summaries
            for csv_path in sorted(cfg_d.glob("maximus_*_metrics_summary_*.csv")):
                all_rows.extend(parse_maximus_summary(csv_path, pl, clk))

            # Sirius summaries
            for csv_path in sorted(cfg_d.glob("sirius_*_metrics_summary_*.csv")):
                all_rows.extend(parse_sirius_summary(csv_path, pl, clk))

    return all_rows


def write_sweep_summary(results_dir: Path, rows: list[dict]) -> Path:
    """Write the combined energy_sweep_summary.csv."""
    out_path = results_dir / "energy_sweep_summary.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"\n  Sweep summary: {out_path} ({len(rows)} rows)")
    return out_path


def print_best_configs(rows: list[dict]) -> None:
    """Print the lowest-energy configuration per (engine, benchmark, sf)."""
    if not rows:
        print("\n  No results to analyze.")
        return

    # Group by (engine, benchmark, sf)
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        if r["status"] != "OK" or not r["energy_j"]:
            continue
        try:
            energy = float(r["energy_j"])
        except (ValueError, TypeError):
            continue
        key = (r["engine"], r["benchmark"], str(r["sf"]))
        groups.setdefault(key, []).append((energy, r))

    print(f"\n{'=' * 80}")
    print("  BEST CONFIGURATIONS (lowest total energy per benchmark)")
    print(f"{'=' * 80}")
    print(f"  {'Engine':<10} {'Benchmark':<12} {'SF':<6} {'PL(W)':<8} {'CLK(MHz)':<10} "
          f"{'Avg E(J)':<10} {'Queries'}")
    print(f"  {'-' * 75}")

    for key in sorted(groups.keys()):
        engine, bench, sf = key
        entries = groups[key]

        # Group by config to get total energy per config
        config_energy: dict[tuple, list[float]] = {}
        for energy, r in entries:
            ck = (int(r["power_limit_w"]), int(r["sm_clock_mhz"]))
            config_energy.setdefault(ck, []).append(energy)

        # Find config with lowest average per-query energy
        best_cfg = None
        best_avg = float("inf")
        best_n = 0
        for ck, energies in config_energy.items():
            avg_e = sum(energies) / len(energies)
            if avg_e < best_avg:
                best_avg = avg_e
                best_cfg = ck
                best_n = len(energies)

        if best_cfg:
            pl, clk = best_cfg
            print(f"  {engine:<10} {bench:<12} {sf:<6} {pl:<8} {clk:<10} "
                  f"{best_avg:<10.2f} {best_n}")

    print()


# ══════════════════════════════════════════════════════════════════════════════
#  Main Sweep Loop
# ══════════════════════════════════════════════════════════════════════════════

def run_sweep(args: argparse.Namespace) -> None:
    """Main sweep loop: iterate (PL, CLK), set GPU config, run full A+B pipeline."""
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    power_limits = args.power_limits
    sm_clocks = args.sm_clocks
    engines = args.engines
    benchmarks = args.benchmarks
    resume = args.resume

    total_configs = len(power_limits) * len(sm_clocks)
    total_engine_configs = total_configs * len(engines)

    print("=" * 80)
    print("  GPU ENERGY SWEEP")
    print(f"  Power limits: {power_limits}")
    print(f"  SM clocks:    {sm_clocks}")
    print(f"  Engines:      {engines}")
    print(f"  Benchmarks:   {benchmarks}")
    print(f"  Results dir:  {results_dir}")
    print(f"  Resume:       {resume}")
    print(f"  Total configs: {total_configs} ({total_engine_configs} engine-configs)")
    print(f"  Started: {datetime.now()}")
    print("=" * 80)

    # Enable persistence mode once at the start
    enable_persistence_mode()

    completed = 0
    skipped = 0
    failed = 0
    start_time = time.time()

    for cfg_idx, (pl, clk) in enumerate(
        [(p, c) for p in power_limits for c in sm_clocks], start=1
    ):
        tag = config_tag(pl, clk)
        cfg_d = config_dir(results_dir, pl, clk)

        print(f"\n{'=' * 80}")
        print(f"  CONFIG {cfg_idx}/{total_configs}: {tag} "
              f"(PL={pl}W, CLK={clk}MHz)")

        # ETA calculation
        elapsed = time.time() - start_time
        done = completed + skipped
        if done > 0:
            avg_per_config = elapsed / done
            remaining = total_configs - cfg_idx + 1
            eta_s = avg_per_config * remaining
            eta_str = str(timedelta(seconds=int(eta_s)))
            print(f"  Progress: {done}/{total_configs} done, ETA: {eta_str}")
        print(f"{'=' * 80}")

        # Resume: check if this config already has all results
        if resume and config_has_results(cfg_d, engines):
            print(f"  SKIP (resume): {tag} already has results")
            skipped += 1
            continue

        # Cool-down check
        wait_for_cooldown()

        # Set GPU config
        if not set_gpu_config(pl, clk):
            print(f"  FAILED to set GPU config for {tag}, skipping")
            failed += 1
            continue

        # Create config directory
        cfg_d.mkdir(parents=True, exist_ok=True)

        # Build benchmark list including microbench variants
        bench_with_micro = list(benchmarks)
        for b in list(benchmarks):
            bench_with_micro.append(f"microbench_{b}")

        has_maximus = "maximus" in engines
        has_sirius = "sirius" in engines
        test_mode = args.test

        def _run_step(step_name, func, *a, **kw):
            print(f"\n  --- {step_name} for {tag} ---")
            try:
                rc = func(*a, **kw)
                if rc != 0:
                    print(f"  WARNING: {step_name} exited with code {rc}")
            except subprocess.TimeoutExpired:
                print(f"  ERROR: {step_name} timed out for {tag}")
            except Exception as e:
                print(f"  ERROR: {step_name} failed for {tag}: {e}")

        # ── Category A: Data on GPU ──────────────────────────────────────
        # A1: Maximus GPU timing
        if has_maximus:
            _run_step("A1 Maximus timing", run_maximus_timing,
                      bench_with_micro, cfg_d, n_reps=3, test_mode=test_mode)

        # A2: Sirius GPU timing
        if has_sirius:
            _run_step("A2 Sirius timing", run_sirius_timing,
                      bench_with_micro, cfg_d, test_mode=test_mode)

        # A3: Maximus GPU metrics (reuse A1 timing CSV)
        if has_maximus:
            a1_csv = cfg_d / "maximus_benchmark.csv"
            _run_step("A3 Maximus metrics", run_maximus_metrics,
                      bench_with_micro, cfg_d, args.maximus_target_time,
                      timing_csv=str(a1_csv), test_mode=test_mode)

        # A4: Sirius GPU metrics
        if has_sirius:
            _run_step("A4 Sirius metrics", run_sirius_metrics,
                      bench_with_micro, cfg_d, args.sirius_target_time,
                      test_mode=test_mode)

        # ── Category B: Data on CPU ──────────────────────────────────────
        # B1: Maximus CPU-data timing
        if has_maximus:
            _run_step("B1 Maximus CPU timing", run_maximus_cpu_timing,
                      bench_with_micro, cfg_d, test_mode=test_mode)

        # B2: Maximus CPU-data metrics (reuse B1 timing CSV)
        if has_maximus:
            b1_csv = cfg_d / "maximus_cpu_data_timing.csv"
            _run_step("B2 Maximus CPU metrics", run_maximus_cpu_metrics,
                      bench_with_micro, cfg_d, args.maximus_target_time,
                      timing_csv=str(b1_csv), test_mode=test_mode)

        # B3: Sirius CPU-data timing + metrics
        if has_sirius:
            _run_step("B3 Sirius CPU data", run_sirius_cpu_data,
                      bench_with_micro, cfg_d, n_reps=10, test_mode=test_mode)

        completed += 1

        # Pause between configs for thermal stability
        print(f"\n  Cooling pause ({COOLDOWN_PAUSE_S}s)...")
        time.sleep(COOLDOWN_PAUSE_S)

    # Aggregate results
    print(f"\n{'=' * 80}")
    print("  AGGREGATING RESULTS")
    print(f"{'=' * 80}")

    all_rows = aggregate_results(results_dir, power_limits, sm_clocks)
    if all_rows:
        write_sweep_summary(results_dir, all_rows)
        print_best_configs(all_rows)
    else:
        print("  No results found to aggregate.")

    # Final summary
    elapsed = time.time() - start_time
    print(f"\n{'=' * 80}")
    print(f"  SWEEP COMPLETE")
    print(f"  Total time:  {timedelta(seconds=int(elapsed))}")
    print(f"  Completed:   {completed}/{total_configs}")
    print(f"  Skipped:     {skipped}")
    print(f"  Failed:      {failed}")
    print(f"  Finished:    {datetime.now()}")
    print(f"{'=' * 80}")


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_int_list(s: str) -> list[int]:
    """Parse comma-separated integers."""
    return [int(x.strip()) for x in s.split(",")]


def main():
    global GPU_ID, _GPU_INFO, DEFAULT_POWER_LIMITS, DEFAULT_SM_CLOCKS

    # Auto-detect GPU hardware
    _GPU_INFO = detect_gpu()
    GPU_ID = str(_GPU_INFO["index"])
    DEFAULT_POWER_LIMITS = gpu_power_levels(_GPU_INFO, n=3)
    DEFAULT_SM_CLOCKS = gpu_sm_clock_levels(_GPU_INFO, n=5)
    print(f"  [HW] Detected GPU #{GPU_ID}: {_GPU_INFO['name']}")
    print(f"  [HW] Power levels: {DEFAULT_POWER_LIMITS}")
    print(f"  [HW] SM clock levels: {DEFAULT_SM_CLOCKS}")

    parser = argparse.ArgumentParser(
        description="GPU energy sweep: explore (power-limit, SM-clock) configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_energy_sweep.py
  python run_energy_sweep.py --power-limits 250,300,360 --sm-clocks 1200,2400
  python run_energy_sweep.py --engines maximus --benchmarks tpch h2o --resume
  python run_energy_sweep.py --results-dir results/sweep_v2 --resume
""",
    )
    parser.add_argument(
        "--power-limits", type=parse_int_list,
        default=DEFAULT_POWER_LIMITS,
        help=f"Comma-separated power limits in watts "
             f"(default: {','.join(map(str, DEFAULT_POWER_LIMITS))})",
    )
    parser.add_argument(
        "--sm-clocks", type=parse_int_list,
        default=DEFAULT_SM_CLOCKS,
        help=f"Comma-separated SM clock frequencies in MHz "
             f"(default: {','.join(map(str, DEFAULT_SM_CLOCKS))})",
    )
    parser.add_argument(
        "--engines", nargs="+", default=["maximus", "sirius"],
        choices=["maximus", "sirius"],
        help="Engines to benchmark (default: maximus sirius)",
    )
    parser.add_argument(
        "--benchmarks", nargs="+",
        default=["tpch", "h2o", "clickbench"],
        choices=["tpch", "h2o", "clickbench"],
        help="Benchmarks to run (default: tpch h2o clickbench)",
    )
    parser.add_argument(
        "--results-dir", type=str,
        default="results/energy_sweep",
        help="Output directory (default: results/energy_sweep)",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Skip configs that already have summary CSVs",
    )
    parser.add_argument(
        "--maximus-target-time", type=float, default=5,
        help="Target sustained time for Maximus in seconds (default: 5)",
    )
    parser.add_argument(
        "--sirius-target-time", type=float, default=5,
        help="Target sustained time for Sirius in seconds (default: 5)",
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Quick test mode: use fewer configs and queries",
    )
    args = parser.parse_args()
    if args.test:
        # In test mode, use only 1 power limit and 1 SM clock
        args.power_limits = [DEFAULT_POWER_LIMITS[-1]]  # default PL only
        args.sm_clocks = [DEFAULT_SM_CLOCKS[-1]]        # max SM clock only

    # Validate: filter out power limits outside GPU's supported range
    pl_min = _GPU_INFO["power_min_w"]
    pl_max = _GPU_INFO["power_max_w"]
    valid_pls = [pl for pl in args.power_limits if pl_min <= pl <= pl_max]
    if len(valid_pls) < len(args.power_limits):
        dropped = set(args.power_limits) - set(valid_pls)
        print(f"  [WARN] Dropped power limits outside GPU range [{pl_min}W, {pl_max}W]: {sorted(dropped)}")
    args.power_limits = valid_pls if valid_pls else [_GPU_INFO["power_default_w"]]

    # Validate: snap SM clocks to nearest supported values
    supported_clocks = _GPU_INFO["sm_clocks"]
    if supported_clocks:
        valid_clks = []
        for clk in args.sm_clocks:
            nearest = min(supported_clocks, key=lambda c: abs(c - clk))
            if nearest not in valid_clks:
                valid_clks.append(nearest)
            else:
                # Already have this value, skip duplicate
                pass
        if valid_clks != args.sm_clocks:
            print(f"  [INFO] SM clocks snapped to supported values: {args.sm_clocks} -> {valid_clks}")
        args.sm_clocks = valid_clks

    # Safety: always restore GPU defaults on exit
    try:
        run_sweep(args)
    except KeyboardInterrupt:
        print("\n\n  INTERRUPTED by user (Ctrl+C)")
    except Exception as e:
        print(f"\n\n  FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        _restore_gpu_defaults()


if __name__ == "__main__":
    main()
