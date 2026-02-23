#!/usr/bin/env python3
"""
Compare Sirius vs Maximus benchmark results.

Usage:
    python compare_results.py --sirius results/sirius_benchmark.csv \
                              --maximus results/maximus_benchmark.csv

Reads CSV outputs from both benchmark runners, normalizes query names
(q01 -> q1), and produces a per-query comparison table grouped by
(benchmark, SF).
"""
from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict


def normalize_query(q: str) -> str:
    """q01 -> q1, q1 -> q1."""
    m = re.match(r"^q0*(\d+)$", q)
    return f"q{m.group(1)}" if m else q


def load_sirius(path: str) -> dict:
    data = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            if row["status"] not in ("OK", "FALLBACK"):
                continue
            key = (row["benchmark"], str(row["sf"]), normalize_query(row["query"]))
            data[key] = float(row["wall_time_s"]) * 1000  # s -> ms
    return data


def load_maximus(path: str) -> dict:
    data = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            if row["status"] != "OK":
                continue
            key = (row["benchmark"], str(row["sf"]), normalize_query(row["query"]))
            data[key] = float(row["min_ms"])
    return data


def main():
    parser = argparse.ArgumentParser(description="Compare Sirius vs Maximus")
    parser.add_argument("--sirius", required=True, help="Sirius CSV")
    parser.add_argument("--maximus", required=True, help="Maximus CSV")
    args = parser.parse_args()

    sirius = load_sirius(args.sirius)
    maximus = load_maximus(args.maximus)

    common = sorted(set(sirius.keys()) & set(maximus.keys()))
    groups: dict[tuple, list] = defaultdict(list)
    for key in common:
        bench, sf, query = key
        s_ms, m_ms = sirius[key], maximus[key]
        if m_ms > 0 and s_ms > 0:
            ratio = s_ms / m_ms
        elif m_ms == 0 and s_ms == 0:
            ratio = 1.0
        elif m_ms == 0:
            ratio = 999
        else:
            ratio = 0.001
        groups[(bench, sf)].append((query, s_ms, m_ms, ratio))

    for (bench, sf), entries in sorted(groups.items()):
        entries.sort(key=lambda x: x[0])
        sirius_wins = sum(1 for _, s, m, r in entries if r < 1.0)
        maximus_wins = sum(1 for _, s, m, r in entries if r > 1.0)
        ties = sum(1 for _, s, m, r in entries if r == 1.0)
        total_s = sum(s for _, s, _, _ in entries)
        total_m = sum(m for _, _, m, _ in entries)

        print(f"\n{'='*80}")
        print(f"  {bench.upper()} SF={sf}  ({len(entries)} queries)")
        print(f"  Sirius wins: {sirius_wins}, Maximus wins: {maximus_wins}, Tie: {ties}")
        print(f"  Total: Sirius {total_s:.0f}ms vs Maximus {total_m:.0f}ms")
        print(f"{'='*80}")
        print(f"  {'Query':<7} {'Sirius':>9} {'Maximus':>9} {'Ratio':>8}  Winner")
        print(f"  {'-'*6:<7} {'-'*9:>9} {'-'*9:>9} {'-'*8:>8}  {'-'*20}")

        for query, s_ms, m_ms, ratio in entries:
            if ratio < 0.9:
                winner = f"Sirius  ({1/ratio:.1f}x)"
            elif ratio > 1.1:
                winner = f"Maximus ({ratio:.1f}x)"
            else:
                winner = "~Tie"
            print(f"  {query:<7} {s_ms:>8.1f}ms {m_ms:>8.1f}ms {ratio:>7.2f}x  {winner}")

    print()


if __name__ == "__main__":
    main()
