#!/usr/bin/env python3
"""Plot frequency sweep results across all benchmarks."""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import csv
from pathlib import Path
from matplotlib.patches import Patch

results_base = Path('/home/xzw/gpu_db/results/freq_sweep')
CONFIGS_META = [
    {'name': 'baseline',  'cpu_perf_pct': 100, 'gpu_clk': 'auto'},
    {'name': 'cpu_low',   'cpu_perf_pct': 18,  'gpu_clk': 'auto'},
    {'name': 'gpu_low',   'cpu_perf_pct': 100, 'gpu_clk': 180},
    {'name': 'both_low',  'cpu_perf_pct': 18,  'gpu_clk': 180},
]

# Load all data
all_rows = []
for cfg in CONFIGS_META:
    cfg_dir = results_base / cfg['name']
    if not cfg_dir.exists():
        continue
    for f in sorted(cfg_dir.glob('*_metrics_summary_*.csv')):
        engine = 'maximus' if f.name.startswith('maximus') else 'sirius'
        with open(f) as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                row['config'] = cfg['name']
                row['engine'] = engine
                all_rows.append(row)

df = pd.DataFrame(all_rows)

# Normalize
df['latency_s'] = pd.to_numeric(df.get('min_ms'), errors='coerce') / 1000
df.loc[df['latency_s'].isna(), 'latency_s'] = pd.to_numeric(df.get('min_s'), errors='coerce')
df['gpu_e_j'] = pd.to_numeric(df.get('energy_j'), errors='coerce')
df.loc[df['gpu_e_j'].isna(), 'gpu_e_j'] = pd.to_numeric(df.get('gpu_energy_j'), errors='coerce')
df['cpu_e_j'] = pd.to_numeric(df['cpu_energy_j'], errors='coerce')
df['cpu_e_j'] = df['cpu_e_j'].clip(lower=0)  # fix RAPL overflow
df['gpu_e_j'] = df['gpu_e_j'].clip(lower=0)
df['total_e_j'] = df['gpu_e_j'] + df['cpu_e_j']
df['gpu_w'] = pd.to_numeric(df['avg_power_w'], errors='coerce')
df['cpu_w'] = pd.to_numeric(df['avg_cpu_pkg_w'], errors='coerce').clip(lower=0)

ok = df[df['status'] == 'OK'].copy()

configs = ['baseline', 'cpu_low', 'gpu_low', 'both_low']
cfg_labels = {
    'baseline': 'Baseline\n100%/auto',
    'cpu_low': 'CPU Low\n18%/auto',
    'gpu_low': 'GPU Low\n100%/180M',
    'both_low': 'Both Low\n18%/180M',
}
engine_colors = {'maximus': '#FF9800', 'sirius': '#2196F3'}
benchmarks = ['tpch', 'h2o', 'clickbench']
bench_labels = {'tpch': 'TPC-H', 'h2o': 'H2O', 'clickbench': 'ClickBench'}

# ── Figure: Per-benchmark comparison (3 rows × 2 cols) ───────────────────────
fig, axes = plt.subplots(3, 2, figsize=(16, 16))
fig.suptitle("Frequency Sweep — All Benchmarks (SF=1-2)\nAvg per-query latency & energy across all queries",
             fontsize=15, fontweight='bold')

for row_idx, bench in enumerate(benchmarks):
    ax_lat = axes[row_idx, 0]
    ax_eng = axes[row_idx, 1]

    x = np.arange(len(configs))
    width = 0.35

    for i, engine in enumerate(['maximus', 'sirius']):
        lat_vals = []
        eng_vals = []
        gpu_e_vals = []
        cpu_e_vals = []
        for cfg in configs:
            subset = ok[(ok['config'] == cfg) & (ok['engine'] == engine) & (ok['benchmark'] == bench)]
            if len(subset) > 0:
                lat_vals.append(subset['latency_s'].mean())
                eng_vals.append(subset['total_e_j'].mean())
                gpu_e_vals.append(subset['gpu_e_j'].mean())
                cpu_e_vals.append(subset['cpu_e_j'].mean())
            else:
                lat_vals.append(0)
                eng_vals.append(0)
                gpu_e_vals.append(0)
                cpu_e_vals.append(0)

        # Latency bars
        bars = ax_lat.bar(x + i * width, lat_vals, width,
                          color=engine_colors[engine], alpha=0.85, label=engine.title())
        for bar, v in zip(bars, lat_vals):
            if v > 0:
                ax_lat.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                            f'{v:.3f}' if v < 1 else f'{v:.1f}',
                            ha='center', va='bottom', fontsize=7, fontweight='bold')

        # Energy bars (stacked GPU + CPU)
        bars_g = ax_eng.bar(x + i * width, gpu_e_vals, width,
                            color=engine_colors[engine], alpha=0.85, label=f'{engine.title()} GPU')
        bars_c = ax_eng.bar(x + i * width, cpu_e_vals, width, bottom=gpu_e_vals,
                            color=engine_colors[engine], alpha=0.3)
        for j, (g, c) in enumerate(zip(gpu_e_vals, cpu_e_vals)):
            t = g + c
            if t > 0:
                ax_eng.text(x[j] + i * width, t,
                            f'{t:.1f}', ha='center', va='bottom', fontsize=7, fontweight='bold')

    ax_lat.set_xticks(x + width / 2)
    ax_lat.set_xticklabels([cfg_labels[c] for c in configs], fontsize=8)
    ax_lat.set_ylabel('Avg Query Latency (s)')
    ax_lat.set_title(f'{bench_labels[bench]} — Latency')
    ax_lat.legend(fontsize=9)
    ax_lat.grid(True, alpha=0.3, axis='y')

    ax_eng.set_xticks(x + width / 2)
    ax_eng.set_xticklabels([cfg_labels[c] for c in configs], fontsize=8)
    ax_eng.set_ylabel('Avg Energy per Query (J)')
    ax_eng.set_title(f'{bench_labels[bench]} — Total Energy (solid=GPU, faded=CPU)')
    legend_e = [Patch(facecolor=engine_colors[e], alpha=0.85, label=e.title()) for e in ['maximus', 'sirius']]
    legend_e.append(Patch(facecolor='gray', alpha=0.3, label='CPU portion'))
    ax_eng.legend(handles=legend_e, fontsize=8)
    ax_eng.grid(True, alpha=0.3, axis='y')

fig.tight_layout(rect=[0, 0, 1, 0.95])
fig.savefig(results_base / 'freq_sweep_comparison.png', dpi=150, bbox_inches='tight')
print('Saved: freq_sweep_comparison.png')
plt.close(fig)

# ── Figure 2: Heatmap of energy ratios ───────────────────────────────────────
combos = [(e, b) for e in ['maximus', 'sirius'] for b in benchmarks]
combo_labels = [f'{e.title()}\n{bench_labels[b]}' for e, b in combos]

fig2, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
fig2.suptitle('Frequency Scaling Impact — All Benchmarks', fontsize=14, fontweight='bold')

for ax, metric, title, cmap, vmin, vmax in [
    (ax1, 'latency_s', 'Latency Ratio vs Baseline', 'RdYlGn_r', 0.5, 15),
    (ax2, 'total_e_j', 'Energy Ratio vs Baseline', 'RdYlGn_r', 0.3, 10),
]:
    data = []
    for eng, bench in combos:
        base = ok[(ok['config'] == 'baseline') & (ok['engine'] == eng) & (ok['benchmark'] == bench)][metric].mean()
        row = []
        for cfg in configs:
            val = ok[(ok['config'] == cfg) & (ok['engine'] == eng) & (ok['benchmark'] == bench)][metric].mean()
            row.append(val / base if base > 0 and not np.isnan(val) else np.nan)
        data.append(row)

    arr = np.array(data)
    im = ax.imshow(arr, cmap=cmap, aspect='auto', vmin=vmin, vmax=vmax)
    for i in range(len(combos)):
        for j in range(len(configs)):
            v = arr[i, j]
            if not np.isnan(v):
                color = 'white' if v > vmax * 0.5 else 'black'
                ax.text(j, i, f'{v:.2f}x', ha='center', va='center',
                        fontsize=10, fontweight='bold', color=color)
            else:
                ax.text(j, i, 'N/A', ha='center', va='center', fontsize=9, color='gray')

    ax.set_xticks(range(len(configs)))
    ax.set_xticklabels(configs, fontsize=9)
    ax.set_yticks(range(len(combos)))
    ax.set_yticklabels(combo_labels, fontsize=8)
    ax.set_title(title)
    plt.colorbar(im, ax=ax, shrink=0.8)

fig2.tight_layout(rect=[0, 0, 1, 0.93])
fig2.savefig(results_base / 'freq_sweep_heatmap.png', dpi=150, bbox_inches='tight')
print('Saved: freq_sweep_heatmap.png')
plt.close(fig2)

# ── Figure 3: CPU power validation ───────────────────────────────────────────
fig3, ax = plt.subplots(figsize=(12, 5))
fig3.suptitle('CPU Power Validation: intel_pstate fix working', fontsize=14, fontweight='bold')

x = np.arange(len(configs))
width = 0.13
for i, (eng, bench) in enumerate(combos):
    vals = []
    for cfg in configs:
        subset = ok[(ok['config'] == cfg) & (ok['engine'] == eng) & (ok['benchmark'] == bench)]
        vals.append(subset['cpu_w'].mean() if len(subset) > 0 else 0)
    bars = ax.bar(x + i * width, vals, width, alpha=0.8,
                  label=f'{eng}/{bench}')

ax.set_xticks(x + width * len(combos) / 2)
ax.set_xticklabels([cfg_labels[c] for c in configs], fontsize=9)
ax.set_ylabel('Avg CPU Package Power (W)')
ax.set_title('CPU Power by Config (should drop ~50% for cpu_low/both_low)')
ax.legend(fontsize=7, ncol=3)
ax.grid(True, alpha=0.3, axis='y')
ax.axhline(y=55, color='red', linestyle='--', alpha=0.5, label='~55W target')

fig3.tight_layout()
fig3.savefig(results_base / 'freq_sweep_cpu_power.png', dpi=150, bbox_inches='tight')
print('Saved: freq_sweep_cpu_power.png')
plt.close(fig3)
