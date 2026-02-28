#!/usr/bin/env python3
"""
Benchmark Metrics Visualization.
Generates comprehensive plots from benchmark timing and GPU telemetry data.

Usage:
    python plot_metrics.py                          # Use default results dir
    python plot_metrics.py --results-dir ./results  # Custom results dir
"""

import argparse
import os
import re
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from pathlib import Path

warnings.filterwarnings('ignore')

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS = SCRIPT_DIR.parent.parent / "results"

DPI = 150
FIGSIZE = (16, 9)

plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams.update({
    'figure.figsize': FIGSIZE,
    'figure.dpi': DPI,
    'axes.titlesize': 16,
    'axes.labelsize': 13,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 11,
    'figure.titlesize': 18,
})


# ---------------------------------------------------------------------------
# Helper: natural sort key for query names
# ---------------------------------------------------------------------------
def query_sort_key(q):
    m = re.match(r'([a-zA-Z_]*)(\d+)(.*)', str(q))
    if m:
        return (m.group(1), int(m.group(2)), m.group(3))
    return (str(q), 0, '')


def grouped_bar(df, x_col, group_col, y_col, ax, colors=None, log_y=False):
    groups = sorted(df[group_col].unique(), key=lambda g: (str(type(g)), g))
    x_vals = sorted(df[x_col].unique(), key=query_sort_key)
    n_groups = len(groups)
    x_pos = np.arange(len(x_vals))
    width = 0.8 / max(n_groups, 1)
    if colors is None:
        cmap = plt.cm.get_cmap('tab10', max(n_groups, 1))
        colors = [cmap(i) for i in range(n_groups)]
    for i, g in enumerate(groups):
        sub = df[df[group_col] == g].set_index(x_col)
        vals = [sub.loc[q, y_col] if q in sub.index else 0 for q in x_vals]
        offset = (i - n_groups / 2 + 0.5) * width
        ax.bar(x_pos + offset, vals, width, label=f'SF={g}', color=colors[i % len(colors)])
    ax.set_xticks(x_pos)
    ax.set_xticklabels(x_vals, rotation=45, ha='right')
    if log_y:
        ax.set_yscale('log')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)


def sf_sort_val(sf):
    s = str(sf)
    if s.endswith('gb'):
        return float(s.replace('gb', ''))
    try:
        return float(s)
    except ValueError:
        return 0


def plot_tpch_timing(timing, plots_dir):
    print("Generating tpch_timing_by_sf.png ...")
    df = timing[(timing['benchmark'] == 'tpch') & (timing['status'] != 'FALLBACK')].copy()
    if df.empty:
        print("  SKIPPED: no data"); return False
    df['sf'] = df['sf'].astype(int)
    fig, ax = plt.subplots(figsize=(18, 9))
    grouped_bar(df, 'query', 'sf', 'single_query_ms', ax, log_y=True)
    ax.set_xlabel('Query'); ax.set_ylabel('Execution Time (ms, log scale)')
    ax.set_title('Sirius TPC-H Query Timing by Scale Factor')
    fig.tight_layout(); fig.savefig(os.path.join(plots_dir, 'tpch_timing_by_sf.png'), dpi=DPI)
    plt.close(fig); print("  OK"); return True


def plot_h2o_timing(timing, plots_dir):
    print("Generating h2o_timing_by_sf.png ...")
    df = timing[(timing['benchmark'] == 'h2o') & (timing['status'] != 'FALLBACK')].copy()
    if df.empty:
        print("  SKIPPED: no data"); return False
    fig, ax = plt.subplots(figsize=(14, 8))
    grouped_bar(df, 'query', 'sf', 'single_query_ms', ax, log_y=False)
    ax.set_xlabel('Query'); ax.set_ylabel('Execution Time (ms)')
    ax.set_title('Sirius H2O Query Timing by Scale Factor')
    fig.tight_layout(); fig.savefig(os.path.join(plots_dir, 'h2o_timing_by_sf.png'), dpi=DPI)
    plt.close(fig); print("  OK"); return True


def plot_clickbench_timing(timing, plots_dir):
    print("Generating clickbench_timing_by_sf.png ...")
    df = timing[(timing['benchmark'] == 'clickbench') & (timing['status'] != 'FALLBACK')].copy()
    if df.empty:
        print("  SKIPPED: no data"); return False
    df['sf'] = df['sf'].astype(int)
    fig, ax = plt.subplots(figsize=(20, 9))
    grouped_bar(df, 'query', 'sf', 'single_query_ms', ax, log_y=True)
    ax.set_xlabel('Query'); ax.set_ylabel('Execution Time (ms, log scale)')
    ax.set_title('Sirius ClickBench Query Timing by Scale Factor')
    fig.tight_layout(); fig.savefig(os.path.join(plots_dir, 'clickbench_timing_by_sf.png'), dpi=DPI)
    plt.close(fig); print("  OK"); return True


def plot_gpu_memory(metrics, benchmark_key, title, out_name, plots_dir):
    print(f"Generating {out_name} ...")
    if benchmark_key not in metrics:
        print(f"  SKIPPED: no metrics data for {benchmark_key}"); return False
    df = metrics[benchmark_key].copy()
    mem_max = df.groupby(['sf', 'query'])['mem_used_mb'].max().reset_index()
    mem_max.columns = ['sf', 'query', 'max_mem_mb']
    queries = sorted(mem_max['query'].unique(), key=query_sort_key)
    sfs = sorted(mem_max['sf'].unique(), key=sf_sort_val)
    n_sf = len(sfs); x_pos = np.arange(len(queries)); width = 0.8 / max(n_sf, 1)
    cmap = plt.cm.get_cmap('tab10', max(n_sf, 1))
    figw = max(14, len(queries) * 0.45)
    fig, ax = plt.subplots(figsize=(figw, 8))
    for i, sf in enumerate(sfs):
        sub = mem_max[mem_max['sf'] == sf].set_index('query')
        vals = [sub.loc[q, 'max_mem_mb'] if q in sub.index else 0 for q in queries]
        offset = (i - n_sf / 2 + 0.5) * width
        ax.bar(x_pos + offset, vals, width, label=f'SF={sf}', color=cmap(i))
    ax.set_xticks(x_pos); ax.set_xticklabels(queries, rotation=45, ha='right')
    ax.set_xlabel('Query'); ax.set_ylabel('Max GPU Memory Used (MB)'); ax.set_title(title)
    ax.legend(); ax.grid(axis='y', alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(plots_dir, out_name), dpi=DPI)
    plt.close(fig); print("  OK"); return True


def plot_gpu_power(metrics, plots_dir):
    print("Generating gpu_power_by_benchmark.png ...")
    all_frames = []
    for key, df in metrics.items():
        sub = df[['sf', 'power_w']].copy(); sub['benchmark'] = key
        sub['label'] = key + ' SF=' + sub['sf'].astype(str); all_frames.append(sub)
    if not all_frames:
        print("  SKIPPED: no metrics data"); return False
    combined = pd.concat(all_frames, ignore_index=True).dropna(subset=['power_w'])
    labels = sorted(combined['label'].unique(), key=lambda x: (x.split()[0], sf_sort_val(x.split('=')[1])))
    fig, ax = plt.subplots(figsize=(16, 9))
    data_for_box = [combined[combined['label'] == lbl]['power_w'].values for lbl in labels]
    bp = ax.boxplot(data_for_box, labels=labels, patch_artist=True, showfliers=False)
    bench_colors = {'tpch': '#4C72B0', 'h2o': '#55A868', 'clickbench': '#C44E52'}
    for i, lbl in enumerate(labels):
        bench = lbl.split()[0]
        bp['boxes'][i].set_facecolor(bench_colors.get(bench, '#999999')); bp['boxes'][i].set_alpha(0.7)
    ax.set_xticklabels(labels, rotation=45, ha='right'); ax.set_ylabel('GPU Power (W)')
    ax.set_title('GPU Power Consumption by Benchmark'); ax.grid(axis='y', alpha=0.3)
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=c, alpha=0.7, label=k.upper()) for k, c in bench_colors.items()]
    ax.legend(handles=legend_elements, loc='upper right')
    fig.tight_layout(); fig.savefig(os.path.join(plots_dir, 'gpu_power_by_benchmark.png'), dpi=DPI)
    plt.close(fig); print("  OK"); return True


def plot_timing_heatmap(timing, plots_dir):
    print("Generating timing_overview_heatmap.png ...")
    df = timing[timing['status'] != 'FALLBACK'].copy()
    if df.empty:
        print("  SKIPPED: no data"); return False
    df['col_label'] = df['benchmark'] + ' SF=' + df['sf'].astype(str)
    pivot = df.pivot_table(index='query', columns='col_label', values='single_query_ms', aggfunc='first')
    pivot = pivot.reindex(sorted(pivot.index, key=query_sort_key))
    def col_sort(c):
        parts = c.split(' SF='); return (parts[0], sf_sort_val(parts[1]))
    pivot = pivot[sorted(pivot.columns, key=col_sort)]
    pivot = pivot.replace(0, np.nan)
    valid_vals = pivot.values[~np.isnan(pivot.values)]
    if len(valid_vals) == 0:
        print("  SKIPPED: all NaN"); return False
    vmin = max(valid_vals.min(), 0.1); vmax = valid_vals.max()
    fig, ax = plt.subplots(figsize=(16, max(12, len(pivot) * 0.22)))
    im = ax.imshow(pivot.values, aspect='auto', cmap='YlOrRd', norm=LogNorm(vmin=vmin, vmax=vmax))
    ax.set_xticks(range(len(pivot.columns))); ax.set_xticklabels(pivot.columns, rotation=45, ha='right', fontsize=9)
    ax.set_yticks(range(len(pivot.index))); ax.set_yticklabels(pivot.index, fontsize=8)
    cbar = fig.colorbar(im, ax=ax, shrink=0.6, pad=0.02); cbar.set_label('Execution Time (ms, log scale)')
    ax.set_title('Query Timing Overview'); ax.set_xlabel('Benchmark + Scale Factor'); ax.set_ylabel('Query')
    fig.tight_layout(); fig.savefig(os.path.join(plots_dir, 'timing_overview_heatmap.png'), dpi=DPI)
    plt.close(fig); print("  OK"); return True


def plot_tpch_scaling(timing, plots_dir):
    print("Generating tpch_scaling.png ...")
    df = timing[(timing['benchmark'] == 'tpch') & (timing['status'] != 'FALLBACK')].copy()
    if df.empty:
        print("  SKIPPED: no data"); return False
    df['sf_num'] = df['sf'].astype(int)
    queries = sorted(df['query'].unique(), key=query_sort_key)
    sfs = sorted(df['sf_num'].unique())
    fig, ax = plt.subplots(figsize=(14, 9))
    cmap = plt.cm.get_cmap('tab20', max(len(queries), 1))
    for i, q in enumerate(queries):
        sub = df[df['query'] == q].sort_values('sf_num')
        if len(sub) < 2: continue
        ax.plot(sub['sf_num'], sub['single_query_ms'], marker='o', markersize=4,
                label=q, color=cmap(i % 20), linewidth=1.5, alpha=0.8)
    sf1_data = df[df['sf_num'] == sfs[0]]
    if not sf1_data.empty:
        median_sf1 = sf1_data['single_query_ms'].median()
        sf_range = np.array(sfs); ideal = median_sf1 * (sf_range / sfs[0])
        ax.plot(sf_range, ideal, 'k--', linewidth=2, alpha=0.5, label='Ideal linear scaling')
    ax.set_xlabel('Scale Factor'); ax.set_ylabel('Execution Time (ms)')
    ax.set_title('TPC-H Query Scaling with Data Size')
    ax.set_xticks(sfs); ax.set_xticklabels([str(s) for s in sfs])
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8, ncol=2); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(plots_dir, 'tpch_scaling.png'), dpi=DPI, bbox_inches='tight')
    plt.close(fig); print("  OK"); return True


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark visualization plots")
    parser.add_argument("--results-dir", type=str, default=str(DEFAULT_RESULTS),
                        help="Directory containing benchmark result CSVs")
    args = parser.parse_args()

    base = args.results_dir
    plots_dir = os.path.join(base, 'plots')
    os.makedirs(plots_dir, exist_ok=True)

    print("Loading data...")
    timing_path = os.path.join(base, 'sirius_timing_per_query.csv')
    if not os.path.exists(timing_path):
        print(f"ERROR: {timing_path} not found"); return
    timing = pd.read_csv(timing_path)

    metrics_files = {'tpch': 'tpch_metrics_samples.csv', 'h2o': 'h2o_metrics_samples.csv', 'clickbench': 'clickbench_metrics_samples.csv'}
    summary_files = {'tpch': 'tpch_metrics_samples_summary.csv', 'h2o': 'h2o_metrics_samples_summary.csv', 'clickbench': 'clickbench_metrics_samples_summary.csv'}
    metrics = {}
    for key, fname in metrics_files.items():
        path = os.path.join(base, fname)
        if os.path.exists(path):
            metrics[key] = pd.read_csv(path); print(f"  Loaded {fname}: {len(metrics[key])} rows")

    results = {}
    print("\n=== Generating Benchmark Plots ===\n")
    results['tpch_timing_by_sf.png'] = plot_tpch_timing(timing, plots_dir)
    results['h2o_timing_by_sf.png'] = plot_h2o_timing(timing, plots_dir)
    results['clickbench_timing_by_sf.png'] = plot_clickbench_timing(timing, plots_dir)
    results['tpch_gpu_memory.png'] = plot_gpu_memory(metrics, 'tpch', 'TPC-H GPU Memory Usage', 'tpch_gpu_memory.png', plots_dir)
    results['h2o_gpu_memory.png'] = plot_gpu_memory(metrics, 'h2o', 'H2O GPU Memory Usage', 'h2o_gpu_memory.png', plots_dir)
    results['clickbench_gpu_memory.png'] = plot_gpu_memory(metrics, 'clickbench', 'ClickBench GPU Memory Usage', 'clickbench_gpu_memory.png', plots_dir)
    results['gpu_power_by_benchmark.png'] = plot_gpu_power(metrics, plots_dir)
    results['timing_overview_heatmap.png'] = plot_timing_heatmap(timing, plots_dir)
    results['tpch_scaling.png'] = plot_tpch_scaling(timing, plots_dir)

    print("\n=== Summary ===")
    for name, ok in results.items():
        print(f"  {name:40s} {'GENERATED' if ok else 'SKIPPED/FAILED'}")
    generated = sum(1 for v in results.values() if v)
    print(f"\n{generated}/{len(results)} plots generated in {plots_dir}/\n")


if __name__ == '__main__':
    main()
