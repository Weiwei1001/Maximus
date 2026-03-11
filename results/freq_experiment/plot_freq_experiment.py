#!/usr/bin/env python3
"""Plot frequency scaling experiment results."""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

SAMPLES = "freq_experiment_samples_20260307_103613.csv"
SUMMARY = "freq_experiment_summary_20260307_103613.csv"

df = pd.read_csv(SAMPLES)
summary = pd.read_csv(SUMMARY)

configs = ["baseline", "cpu_low", "gpu_low", "both_low"]
labels = {
    "baseline": "Baseline\n(CPU=4.4GHz, GPU=auto)",
    "cpu_low":  "CPU Low\n(CPU=0.8GHz, GPU=auto)",
    "gpu_low":  "GPU Low\n(CPU=4.4GHz, GPU=180MHz)",
    "both_low": "Both Low\n(CPU=0.8GHz, GPU=180MHz)",
}
colors = {"baseline": "#2196F3", "cpu_low": "#FF9800", "gpu_low": "#4CAF50", "both_low": "#F44336"}

fig = plt.figure(figsize=(18, 14))
fig.suptitle("Frequency Scaling Experiment — Sirius TPC-H SF=5 Q1 (30 reps)",
             fontsize=16, fontweight="bold", y=0.98)

gs = gridspec.GridSpec(3, 2, hspace=0.35, wspace=0.3, top=0.93, bottom=0.06, left=0.07, right=0.97)

# ── Row 1: Power traces (GPU left, CPU right) ────────────────────────────────
ax_gpu_power = fig.add_subplot(gs[0, 0])
ax_cpu_power = fig.add_subplot(gs[0, 1])

for cfg in configs:
    d = df[df["config"] == cfg]
    t = d["time_offset_ms"].values / 1000.0
    ax_gpu_power.plot(t, d["gpu_power_w"], color=colors[cfg], alpha=0.8, linewidth=1.2, label=cfg)
    ax_cpu_power.plot(t, d["cpu_pkg_w"], color=colors[cfg], alpha=0.8, linewidth=1.2, label=cfg)

ax_gpu_power.set_xlabel("Time (s)")
ax_gpu_power.set_ylabel("GPU Power (W)")
ax_gpu_power.set_title("GPU Power Trace")
ax_gpu_power.legend(fontsize=9)
ax_gpu_power.grid(True, alpha=0.3)

ax_cpu_power.set_xlabel("Time (s)")
ax_cpu_power.set_ylabel("CPU Package Power (W)")
ax_cpu_power.set_title("CPU Power Trace")
ax_cpu_power.legend(fontsize=9)
ax_cpu_power.grid(True, alpha=0.3)

# ── Row 2: GPU util + SM clock traces ────────────────────────────────────────
ax_util = fig.add_subplot(gs[1, 0])
ax_clk = fig.add_subplot(gs[1, 1])

for cfg in configs:
    d = df[df["config"] == cfg]
    t = d["time_offset_ms"].values / 1000.0
    ax_util.plot(t, d["gpu_util_pct"], color=colors[cfg], alpha=0.8, linewidth=1.2, label=cfg)
    ax_clk.plot(t, d["sm_clk_mhz"], color=colors[cfg], alpha=0.8, linewidth=1.2, label=cfg)

ax_util.set_xlabel("Time (s)")
ax_util.set_ylabel("GPU Utilization (%)")
ax_util.set_title("GPU Utilization Trace")
ax_util.legend(fontsize=9)
ax_util.grid(True, alpha=0.3)
ax_util.set_ylim(-5, 105)

ax_clk.set_xlabel("Time (s)")
ax_clk.set_ylabel("SM Clock (MHz)")
ax_clk.set_title("GPU SM Clock Trace")
ax_clk.legend(fontsize=9)
ax_clk.grid(True, alpha=0.3)

# ── Row 3: Bar charts — Timing and Energy ────────────────────────────────────
ax_time = fig.add_subplot(gs[2, 0])
ax_energy = fig.add_subplot(gs[2, 1])

x = np.arange(len(configs))
width = 0.55

# Timing bars
times = [float(summary[summary["config"] == c]["metrics_min_s"].values[0]) for c in configs]
bars = ax_time.bar(x, times, width, color=[colors[c] for c in configs], edgecolor="black", linewidth=0.5)
for bar, t in zip(bars, times):
    ax_time.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(times)*0.02,
                 f"{t:.3f}s", ha="center", va="bottom", fontsize=10, fontweight="bold")
ax_time.set_xticks(x)
ax_time.set_xticklabels([labels[c] for c in configs], fontsize=8)
ax_time.set_ylabel("Query Latency (s)")
ax_time.set_title("Min Query Time (lower = better)")
ax_time.grid(True, alpha=0.3, axis="y")

# Energy bars (stacked GPU + CPU)
gpu_e = [float(summary[summary["config"] == c]["gpu_energy_j"].values[0]) for c in configs]
cpu_e = [float(summary[summary["config"] == c]["cpu_energy_j"].values[0]) for c in configs]

bars_gpu = ax_energy.bar(x, gpu_e, width, color=[colors[c] for c in configs],
                          edgecolor="black", linewidth=0.5, label="GPU Energy")
bars_cpu = ax_energy.bar(x, cpu_e, width, bottom=gpu_e, color=[colors[c] for c in configs],
                          edgecolor="black", linewidth=0.5, alpha=0.4, label="CPU Energy")
for i, (ge, ce) in enumerate(zip(gpu_e, cpu_e)):
    total = ge + ce
    ax_energy.text(x[i], total + max(g+c for g,c in zip(gpu_e, cpu_e))*0.02,
                   f"{total:.1f}J", ha="center", va="bottom", fontsize=10, fontweight="bold")
    ax_energy.text(x[i], ge/2, f"GPU\n{ge:.1f}J", ha="center", va="center", fontsize=7, color="white", fontweight="bold")
    ax_energy.text(x[i], ge + ce/2, f"CPU\n{ce:.1f}J", ha="center", va="center", fontsize=7)

ax_energy.set_xticks(x)
ax_energy.set_xticklabels([labels[c] for c in configs], fontsize=8)
ax_energy.set_ylabel("Energy per Query (J)")
ax_energy.set_title("Energy per Query (GPU + CPU, lower = better)")
ax_energy.legend(fontsize=9)
ax_energy.grid(True, alpha=0.3, axis="y")

plt.savefig("freq_experiment_results.png", dpi=150, bbox_inches="tight")
print("Saved: freq_experiment_results.png")
plt.close()
