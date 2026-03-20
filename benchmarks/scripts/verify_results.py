#!/usr/bin/env python3
"""
Verify benchmark results against a baseline.

Parses log files from a run_all_benchmarks.sh execution and compares
latency and energy against baseline CSVs. Generates a comparison report.

Usage:
    python verify_results.py --log-dir results/logs_YYYYMMDD_HHMMSS
    python verify_results.py --log-dir results/logs_YYYYMMDD_HHMMSS \
        --baseline-dir results/baseline
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
MAXIMUS_DIR = SCRIPT_DIR.parent.parent
DEFAULT_BASELINE_DIR = MAXIMUS_DIR / "results" / "baseline"

# ── Regex patterns ──
RE_SECTION = re.compile(r'(?:METRICS:\s+)?(\S+)\s+SF=(\S+)')
RE_MAXIMUS_TIMING = re.compile(
    r'(\S+): min=([\d.]+)ms avg=([\d.]+)ms \[(OK|FAIL)\]')
RE_SIRIUS_TIMING = re.compile(
    r'(\S+): ([\d.]+)s \[(OK|FALLBACK|ERROR)\](?:\s+\(passes: \[([^\]]+)\]\))?')
RE_MAXIMUS_METRICS = re.compile(
    r'(\S+) \(\d+ reps.*?\.\.\. ([\d.]+)ms, [\d.]+s, GPU:(\d+)W CPU:(\d+)W, '
    r'(\d+)%util, \d+MB, GPU_E:([\d.]+)J CPU_E:([\d.]+)J \[(OK|FAIL)\]')
RE_SIRIUS_METRICS = re.compile(
    r'(\S+) \(\d+ reps/pass.*?\.\.\. ([\d.]+)s, [\d.]+s \(\d+ passes\), '
    r'GPU:(\d+)W CPU:(\d+)W, (\d+)%util, \d+MB, GPU_E:([\d.]+)J CPU_E:([\d.]+)J \[(OK|FAIL)\]')
# B2 log format: simpler, no %util/MB/GPU_E fields — energy estimated as power × latency
RE_MAXIMUS_CPU_METRICS = re.compile(
    r'(\S+) \(\d+ reps, -s cpu\)\.\.\. ([\d.]+)ms, [\d.]+s, GPU:(\d+)W CPU:(\d+)W \[(OK|FAIL)\]')


def _normalize_metrics_text(text: str) -> str:
    """Remove memory leak warnings and rejoin split metrics lines.

    When a memory leak is detected, the output is split across lines:
        q3 (746 reps, -s gpu)...
        ⚠ MEMORY LEAK DETECTED for q3: +2874MB (48505→51379MB)
        60.000ms, 37.0s, GPU:205W ...
    This function removes the warning lines and joins the timing data
    back onto the query line so that regexes can match.
    """
    # Remove memory leak warning lines
    lines = [l for l in text.split('\n') if '⚠ MEMORY LEAK DETECTED' not in l]
    # Rejoin split lines: if a line ends with "..." and the next starts with timing data
    joined = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.rstrip().endswith('...') and i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            # Timing data starts with digits (e.g. "60.000ms," or "0.003s,")
            if re.match(r'[\d.]+(?:ms|s),', next_line):
                joined.append(line.rstrip() + ' ' + next_line)
                i += 2
                continue
        joined.append(line)
        i += 1
    return '\n'.join(joined)


def parse_sections(text):
    lines = text.splitlines()
    sections = []
    current_bench = current_sf = None
    current_lines = []
    for line in lines:
        m = RE_SECTION.search(line)
        if m:
            if current_bench:
                sections.append((current_bench.lower(), current_sf, '\n'.join(current_lines)))
            current_bench = m.group(1)
            current_sf = m.group(2)
            current_lines = []
        else:
            current_lines.append(line)
    if current_bench:
        sections.append((current_bench.lower(), current_sf, '\n'.join(current_lines)))
    return sections


def parse_latency_from_logs(log_dir: Path):
    """Parse A1, A2, B1 logs into latency rows."""
    rows = []
    for fname, engine, storage, regex, is_sirius in [
        ("A1_maximus_timing.log", "maximus", "gpu", RE_MAXIMUS_TIMING, False),
        ("B1_maximus_cpu_timing.log", "maximus", "cpu", RE_MAXIMUS_TIMING, False),
        ("A2_sirius_timing.log", "sirius", "gpu", RE_SIRIUS_TIMING, True),
    ]:
        path = log_dir / fname
        if not path.exists():
            continue
        text = path.read_text()
        for bench, sf, section in parse_sections(text):
            if is_sirius:
                for m in RE_SIRIUS_TIMING.finditer(section):
                    query, time_s, status, passes_str = m.groups()
                    time_ms = float(time_s) * 1000
                    if passes_str:
                        passes = [float(x.strip()) * 1000 for x in passes_str.split(',')]
                        min_ms = round(min(passes), 3)
                        all_times = ";".join(f"{t:.2f}" for t in passes)
                    else:
                        min_ms = round(time_ms, 3)
                        all_times = f"{time_ms:.2f}"
                    rows.append({
                        'engine': engine, 'storage': storage, 'benchmark': bench,
                        'sf': sf, 'query': query,
                        'min_ms': min_ms, 'avg_ms': round(time_ms, 3),
                        'all_times_ms': all_times,
                        'status': status,
                    })
            else:
                for m in regex.finditer(section):
                    query, min_ms, avg_ms, status = m.groups()
                    rows.append({
                        'engine': engine, 'storage': storage, 'benchmark': bench,
                        'sf': sf, 'query': query,
                        'min_ms': float(min_ms), 'avg_ms': float(avg_ms),
                        'all_times_ms': '',  # Maximus timing log doesn't show all reps
                        'status': status,
                    })
    return rows


def parse_energy_from_logs(log_dir: Path):
    """Parse A3, A4, B2 logs into energy rows."""
    rows = []
    for fname, engine, storage, regex, is_sirius in [
        ("A3_maximus_metrics.log", "maximus", "gpu", RE_MAXIMUS_METRICS, False),
        ("A4_sirius_metrics.log", "sirius", "gpu", RE_SIRIUS_METRICS, True),
    ]:
        path = log_dir / fname
        if not path.exists():
            continue
        text = _normalize_metrics_text(path.read_text())
        for bench, sf, section in parse_sections(text):
            for m in regex.finditer(section):
                if is_sirius:
                    query, latency_s, gpu_w, cpu_w, util, gpu_e, cpu_e, status = m.groups()
                    latency_ms = round(float(latency_s) * 1000, 3)
                else:
                    query, latency_ms_str, gpu_w, cpu_w, util, gpu_e, cpu_e, status = m.groups()
                    latency_ms = float(latency_ms_str)
                rows.append({
                    'engine': engine, 'storage': storage, 'benchmark': bench,
                    'sf': sf, 'query': query, 'latency_ms': latency_ms,
                    'gpu_power_w': int(gpu_w), 'gpu_energy_j': float(gpu_e),
                    'status': status,
                })

    # B2: Maximus CPU metrics (no GPU_E field — estimate as gpu_power × latency)
    b2_path = log_dir / "B2_maximus_cpu_metrics.log"
    if b2_path.exists():
        text = _normalize_metrics_text(b2_path.read_text())
        for bench, sf, section in parse_sections(text):
            for m in RE_MAXIMUS_CPU_METRICS.finditer(section):
                query, latency_ms_str, gpu_w, cpu_w, status = m.groups()
                latency_ms = float(latency_ms_str)
                gpu_power = int(gpu_w)
                gpu_energy = round(gpu_power * latency_ms / 1000, 4)
                rows.append({
                    'engine': 'maximus', 'storage': 'cpu', 'benchmark': bench,
                    'sf': sf, 'query': query, 'latency_ms': latency_ms,
                    'gpu_power_w': gpu_power, 'gpu_energy_j': gpu_energy,
                    'status': status,
                })
    return rows


def load_baseline_csv(path: Path, key_fields, value_field):
    """Load baseline CSV into a dict keyed by (engine, storage, benchmark, sf, query)."""
    data = {}
    if not path.exists():
        return data
    with open(path) as f:
        for row in csv.DictReader(f):
            if row.get('status', 'OK') not in ('OK', 'FALLBACK'):
                continue
            key = tuple(row[k] for k in key_fields)
            data[key] = float(row[value_field])
    return data


def pct_diff(new, old):
    if old == 0:
        return float('inf') if new != 0 else 0.0
    return (new - old) / old * 100


def main():
    parser = argparse.ArgumentParser(description="Verify benchmark results against baseline")
    parser.add_argument("--log-dir", required=True, help="Directory with log files from run_all_benchmarks.sh")
    parser.add_argument("--baseline-dir", default=str(DEFAULT_BASELINE_DIR),
                        help="Directory with baseline CSVs (default: results/baseline/)")
    parser.add_argument("--output", default=None,
                        help="Output report file (default: <log-dir>/verification_report.txt)")
    parser.add_argument("--threshold", type=float, default=20.0,
                        help="Warn if latency differs by more than this %% (default: 20)")
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    baseline_dir = Path(args.baseline_dir)
    output_path = Path(args.output) if args.output else log_dir / "verification_report.txt"
    threshold = args.threshold

    if not log_dir.exists():
        print(f"ERROR: log directory not found: {log_dir}")
        sys.exit(1)

    baseline_latency_path = baseline_dir / "baseline_latency.csv"
    baseline_energy_path = baseline_dir / "baseline_energy.csv"

    if not baseline_latency_path.exists():
        print(f"ERROR: baseline latency not found: {baseline_latency_path}")
        sys.exit(1)

    # Parse new results
    new_latency = parse_latency_from_logs(log_dir)
    new_energy = parse_energy_from_logs(log_dir)

    # Load baselines
    lat_key = ['engine', 'storage', 'benchmark', 'sf', 'query']
    base_lat = load_baseline_csv(baseline_latency_path, lat_key, 'min_ms')
    ene_key = ['engine', 'storage', 'benchmark', 'sf', 'query']
    base_ene = load_baseline_csv(baseline_energy_path, ene_key, 'gpu_energy_j')

    lines = []
    def out(s=""):
        lines.append(s)
        print(s)

    out("=" * 80)
    out("  BENCHMARK VERIFICATION REPORT")
    out(f"  New results:  {log_dir}")
    out(f"  Baseline:     {baseline_dir}")
    out(f"  Threshold:    ±{threshold}%")
    out("=" * 80)

    # ── Latency comparison ──
    out("\n" + "=" * 80)
    out("  LATENCY COMPARISON (min_ms)")
    out("=" * 80)

    warnings = 0
    matched = 0
    missing_baseline = 0
    new_queries = 0

    # Group by (engine, storage, benchmark, sf)
    from collections import defaultdict
    groups = defaultdict(list)
    for r in new_latency:
        if r['status'] not in ('OK', 'FALLBACK'):
            continue
        key = (r['engine'], r['storage'], r['benchmark'], r['sf'])
        groups[key].append(r)

    for group_key in sorted(groups.keys()):
        engine, storage, bench, sf = group_key
        entries = groups[group_key]
        out(f"\n  {engine.upper()} ({storage}) {bench.upper()} SF={sf}")
        out(f"  {'Query':<12} {'New':>9} {'Base':>9} {'Diff':>8}  Status")
        out(f"  {'-'*11:<12} {'-'*9:>9} {'-'*9:>9} {'-'*8:>8}  {'-'*12}")

        for r in sorted(entries, key=lambda x: x['query']):
            bkey = (r['engine'], r['storage'], r['benchmark'], r['sf'], r['query'])
            new_val = r['min_ms']
            if bkey in base_lat:
                old_val = base_lat[bkey]
                diff = pct_diff(new_val, old_val)
                matched += 1
                flag = ""
                if abs(diff) > threshold:
                    flag = " ⚠ WARN"
                    warnings += 1
                out(f"  {r['query']:<12} {new_val:>8.1f}ms {old_val:>8.1f}ms {diff:>+7.1f}%{flag}")
            else:
                new_queries += 1
                out(f"  {r['query']:<12} {new_val:>8.1f}ms {'N/A':>9} {'NEW':>8}")

    # ── Energy comparison ──
    out("\n" + "=" * 80)
    out("  ENERGY COMPARISON (gpu_energy_j)")
    out("=" * 80)

    energy_warnings = 0
    energy_matched = 0

    groups_e = defaultdict(list)
    for r in new_energy:
        if r['status'] not in ('OK', 'FALLBACK'):
            continue
        key = (r['engine'], r['storage'], r['benchmark'], r['sf'])
        groups_e[key].append(r)

    for group_key in sorted(groups_e.keys()):
        engine, storage, bench, sf = group_key
        entries = groups_e[group_key]
        out(f"\n  {engine.upper()} {bench.upper()} SF={sf}")
        out(f"  {'Query':<12} {'NewE(J)':>9} {'BaseE(J)':>9} {'Diff':>8}  Status")
        out(f"  {'-'*11:<12} {'-'*9:>9} {'-'*9:>9} {'-'*8:>8}  {'-'*12}")

        for r in sorted(entries, key=lambda x: x['query']):
            bkey = (r['engine'], r['storage'], r['benchmark'], r['sf'], r['query'])
            new_val = r['gpu_energy_j']
            if bkey in base_ene:
                old_val = base_ene[bkey]
                diff = pct_diff(new_val, old_val)
                energy_matched += 1
                flag = ""
                if abs(diff) > threshold:
                    flag = " ⚠ WARN"
                    energy_warnings += 1
                out(f"  {r['query']:<12} {new_val:>8.4f}J {old_val:>8.4f}J {diff:>+7.1f}%{flag}")
            else:
                out(f"  {r['query']:<12} {new_val:>8.4f}J {'N/A':>9} {'NEW':>8}")

    # ── Summary ──
    out("\n" + "=" * 80)
    out("  SUMMARY")
    out("=" * 80)
    out(f"  Latency: {matched} compared, {warnings} warnings (>{threshold}% diff), {new_queries} new")
    out(f"  Energy:  {energy_matched} compared, {energy_warnings} warnings (>{threshold}% diff)")

    total_w = warnings + energy_warnings
    if total_w == 0:
        out(f"\n  ✓ ALL RESULTS WITHIN ±{threshold}% OF BASELINE")
    else:
        out(f"\n  ✗ {total_w} results exceed ±{threshold}% threshold")

    out("=" * 80)

    # Write report
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text('\n'.join(lines) + '\n')
    print(f"\nReport saved to: {output_path}")

    # Also save parsed results as CSVs alongside the report
    lat_csv = log_dir / "test_latency.csv"
    with open(lat_csv, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['engine', 'storage', 'benchmark', 'sf', 'query',
                                          'min_ms', 'avg_ms', 'all_times_ms', 'status'])
        w.writeheader()
        w.writerows([r for r in new_latency if r['status'] in ('OK', 'FALLBACK')])

    ene_csv = log_dir / "test_energy.csv"
    with open(ene_csv, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['engine', 'storage', 'benchmark', 'sf', 'query', 'latency_ms', 'gpu_power_w', 'gpu_energy_j', 'status'])
        w.writeheader()
        w.writerows([r for r in new_energy if r['status'] in ('OK', 'FALLBACK')])

    print(f"Latency CSV: {lat_csv} ({len(new_latency)} rows)")
    print(f"Energy CSV:  {ene_csv} ({len(new_energy)} rows)")

    return 1 if total_w > 0 else 0


if __name__ == '__main__':
    sys.exit(main())
