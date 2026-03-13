#!/usr/bin/env python3
"""Parse test-mode log files and output latency + energy CSVs."""
import re
import csv
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent

# ── Regex patterns ──
# A1/B1: "q1: min=11.0ms avg=25.3ms [OK]"
RE_MAXIMUS_TIMING = re.compile(
    r'(\S+): min=([\d.]+)ms avg=([\d.]+)ms \[(OK|FAIL)\]')
# A2: "q1: 0.342s [OK]  (passes: [0.571, 0.337, 0.342])"
RE_SIRIUS_TIMING = re.compile(
    r'(\S+): ([\d.]+)s \[(OK|FALLBACK|ERROR)\]\s+\(passes: \[([^\]]+)\]\)')
# A3: "q1 (910 reps, -s gpu)... 11.000ms, 17.6s, GPU:175W CPU:0W, 100%util, 40817MB, GPU_E:1.9232J CPU_E:0.0000J [OK]"
RE_MAXIMUS_METRICS = re.compile(
    r'(\S+) \(\d+ reps.*?\.\.\. ([\d.]+)ms, [\d.]+s, GPU:(\d+)W CPU:(\d+)W, (\d+)%util, \d+MB, GPU_E:([\d.]+)J CPU_E:([\d.]+)J \[(OK|FAIL)\]')
# A4: "q1 (3000 reps/pass, target=60.0s)... 0.020s, 73.9s (1 passes), GPU:76W CPU:0W, 100%util, 81149MB, GPU_E:1.5J CPU_E:0.0J [OK]"
RE_SIRIUS_METRICS = re.compile(
    r'(\S+) \(\d+ reps/pass.*?\.\.\. ([\d.]+)s, [\d.]+s \(\d+ passes\), GPU:(\d+)W CPU:(\d+)W, (\d+)%util, \d+MB, GPU_E:([\d.]+)J CPU_E:([\d.]+)J \[(OK|FAIL)\]')
# Section header: "  TPCH SF=1 (3 queries, 3 reps)" or "  METRICS: TPCH SF=1"
RE_SECTION = re.compile(r'(?:METRICS:\s+)?(\S+)\s+SF=(\S+)')


def parse_sections(text):
    """Split text into (benchmark, sf) sections."""
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


def parse_a1(text):
    """Parse Maximus GPU timing log."""
    rows = []
    for bench, sf, section in parse_sections(text):
        for m in RE_MAXIMUS_TIMING.finditer(section):
            query, min_ms, avg_ms, status = m.groups()
            rows.append({
                'engine': 'maximus', 'storage': 'gpu', 'benchmark': bench,
                'sf': sf, 'query': query, 'min_ms': float(min_ms),
                'avg_ms': float(avg_ms), 'status': status,
            })
    return rows


def parse_a2(text):
    """Parse Sirius GPU timing log."""
    rows = []
    for bench, sf, section in parse_sections(text):
        for m in RE_SIRIUS_TIMING.finditer(section):
            query, time_s, status, passes_str = m.groups()
            time_ms = float(time_s) * 1000
            passes = [float(x.strip()) * 1000 for x in passes_str.split(',')]
            min_ms = min(passes)
            rows.append({
                'engine': 'sirius', 'storage': 'gpu', 'benchmark': bench,
                'sf': sf, 'query': query, 'min_ms': round(min_ms, 3),
                'avg_ms': round(time_ms, 3), 'status': status,
            })
    return rows


def parse_b1(text):
    """Parse Maximus CPU-data timing log."""
    rows = []
    for bench, sf, section in parse_sections(text):
        for m in RE_MAXIMUS_TIMING.finditer(section):
            query, min_ms, avg_ms, status = m.groups()
            rows.append({
                'engine': 'maximus', 'storage': 'cpu', 'benchmark': bench,
                'sf': sf, 'query': query, 'min_ms': float(min_ms),
                'avg_ms': float(avg_ms), 'status': status,
            })
    return rows


def parse_a3(text):
    """Parse Maximus GPU metrics log."""
    rows = []
    for bench, sf, section in parse_sections(text):
        for m in RE_MAXIMUS_METRICS.finditer(section):
            query, latency_ms, gpu_w, cpu_w, util, gpu_e, cpu_e, status = m.groups()
            rows.append({
                'engine': 'maximus', 'storage': 'gpu', 'benchmark': bench,
                'sf': sf, 'query': query, 'latency_ms': float(latency_ms),
                'gpu_power_w': int(gpu_w), 'cpu_power_w': int(cpu_w),
                'gpu_util_pct': int(util),
                'gpu_energy_j': float(gpu_e), 'cpu_energy_j': float(cpu_e),
                'status': status,
            })
    return rows


def parse_a4(text):
    """Parse Sirius GPU metrics log."""
    rows = []
    for bench, sf, section in parse_sections(text):
        for m in RE_SIRIUS_METRICS.finditer(section):
            query, latency_s, gpu_w, cpu_w, util, gpu_e, cpu_e, status = m.groups()
            rows.append({
                'engine': 'sirius', 'storage': 'gpu', 'benchmark': bench,
                'sf': sf, 'query': query, 'latency_ms': round(float(latency_s) * 1000, 3),
                'gpu_power_w': int(gpu_w), 'cpu_power_w': int(cpu_w),
                'gpu_util_pct': int(util),
                'gpu_energy_j': float(gpu_e), 'cpu_energy_j': float(cpu_e),
                'status': status,
            })
    return rows


def main():
    # Read all logs
    a1 = (LOG_DIR / 'A1_maximus_timing.log').read_text()
    a2 = (LOG_DIR / 'A2_sirius_timing.log').read_text()
    a3 = (LOG_DIR / 'A3_maximus_metrics.log').read_text()
    a4 = (LOG_DIR / 'A4_sirius_metrics.log').read_text()
    b1 = (LOG_DIR / 'B1_maximus_cpu_timing.log').read_text()

    # ── File 1: Latency ──
    latency_rows = []
    for r in parse_a1(a1):
        latency_rows.append(r)
    for r in parse_a2(a2):
        latency_rows.append(r)
    for r in parse_b1(b1):
        latency_rows.append(r)

    latency_path = LOG_DIR / 'test_latency.csv'
    fields = ['engine', 'storage', 'benchmark', 'sf', 'query', 'min_ms', 'avg_ms', 'status']
    with open(latency_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(latency_rows)
    print(f"Latency: {latency_path} ({len(latency_rows)} rows)")

    # ── File 2: Energy ──
    energy_rows = []
    for r in parse_a3(a3):
        energy_rows.append(r)
    for r in parse_a4(a4):
        energy_rows.append(r)

    energy_path = LOG_DIR / 'test_energy.csv'
    fields_e = ['engine', 'storage', 'benchmark', 'sf', 'query', 'latency_ms',
                'gpu_power_w', 'cpu_power_w', 'gpu_util_pct',
                'gpu_energy_j', 'cpu_energy_j', 'status']
    with open(energy_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields_e)
        w.writeheader()
        w.writerows(energy_rows)
    print(f"Energy: {energy_path} ({len(energy_rows)} rows)")

    # Print summary
    print(f"\n{'='*60}")
    print("LATENCY SUMMARY")
    print(f"{'='*60}")
    for engine in ['maximus', 'sirius']:
        for storage in ['gpu', 'cpu']:
            subset = [r for r in latency_rows if r['engine'] == engine and r['storage'] == storage]
            if not subset:
                continue
            ok = sum(1 for r in subset if r['status'] == 'OK')
            print(f"  {engine} ({storage}): {ok}/{len(subset)} OK")
            benchmarks = sorted(set(r['benchmark'] for r in subset))
            for b in benchmarks:
                b_rows = [r for r in subset if r['benchmark'] == b]
                sfs = sorted(set(r['sf'] for r in b_rows))
                for sf in sfs:
                    sf_rows = [r for r in b_rows if r['sf'] == sf]
                    for r in sf_rows:
                        print(f"    {b} SF={sf} {r['query']}: min={r['min_ms']}ms avg={r['avg_ms']}ms [{r['status']}]")

    print(f"\n{'='*60}")
    print("ENERGY SUMMARY")
    print(f"{'='*60}")
    for engine in ['maximus', 'sirius']:
        subset = [r for r in energy_rows if r['engine'] == engine]
        if not subset:
            continue
        ok = sum(1 for r in subset if r['status'] == 'OK')
        print(f"  {engine}: {ok}/{len(subset)} OK")
        benchmarks = sorted(set(r['benchmark'] for r in subset))
        for b in benchmarks:
            b_rows = [r for r in subset if r['benchmark'] == b]
            sfs = sorted(set(r['sf'] for r in b_rows))
            for sf in sfs:
                sf_rows = [r for r in b_rows if r['sf'] == sf]
                for r in sf_rows:
                    print(f"    {b} SF={sf} {r['query']}: {r['latency_ms']}ms, {r['gpu_power_w']}W, E={r['gpu_energy_j']}J [{r['status']}]")


if __name__ == '__main__':
    main()
