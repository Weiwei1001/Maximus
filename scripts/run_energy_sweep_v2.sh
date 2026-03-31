#!/bin/bash
# Energy Sweep V2: 6 configs × 2 engines, then Maximus data-on-CPU
# Configs: (PL=250,CLK=1800), (PL=250,CLK=2400), (PL=300,CLK=2400),
#          (PL=360,CLK=2400), (PL=360,CLK=3090), (PL=450,CLK=3090)
set -e

cd /home/xzw/gpu_db

RESULTS_GPU="results/energy_sweep_v2"
RESULTS_CPU="results/energy_sweep_v2_cpu"

echo "=============================================="
echo "  Phase 1: GPU storage (Maximus + Sirius)"
echo "=============================================="
python benchmarks/scripts/run_energy_sweep.py \
    --power-limits 250,300,360,450 \
    --sm-clocks 1800,2400,3090 \
    --benchmarks tpch h2o \
    --engines maximus sirius \
    --results-dir "$RESULTS_GPU" \
    --storage gpu

echo ""
echo "=============================================="
echo "  Phase 2: CPU storage (Maximus only)"
echo "=============================================="
python benchmarks/scripts/run_energy_sweep.py \
    --power-limits 250,300,360,450 \
    --sm-clocks 1800,2400,3090 \
    --benchmarks tpch h2o \
    --engines maximus \
    --results-dir "$RESULTS_CPU" \
    --storage cpu

echo ""
echo "=============================================="
echo "  ALL DONE"
echo "=============================================="
