#!/usr/bin/env bash
#
# GPU/CPU Settings Control Script
# ================================
# Knobs used in the energy sweep and frequency sweep experiments.
#
# Usage:
#   ./gpu_settings.sh status              # Show current settings
#   ./gpu_settings.sh set <preset>        # Apply a preset config
#   ./gpu_settings.sh reset               # Restore all defaults
#   ./gpu_settings.sh set-gpu-pl <watts>  # Set GPU power limit only
#   ./gpu_settings.sh set-gpu-clk <mhz>  # Lock GPU SM clock only
#   ./gpu_settings.sh set-gpu-mem <mhz>  # Lock GPU memory clock only
#   ./gpu_settings.sh set-cpu-pct <pct>  # Set CPU max_perf_pct only
#   ./gpu_settings.sh set-cpu-turbo <0|1> # Set CPU turbo (0=on, 1=off)
#
# Presets:
#   default       PL=360W, GPU clocks unlocked, CPU=100%, turbo=on
#   low_power     PL=250W, SM=1800MHz, CPU=100%, turbo=on
#   low_clock     PL=360W, SM=600MHz, CPU=100%, turbo=on
#   high_perf     PL=450W, SM=3090MHz, CPU=100%, turbo=on
#   cpu_low       PL=360W, GPU unlocked, CPU=18% (~800MHz), turbo=off
#   gpu_low       PL=360W, SM=180MHz, CPU=100%, turbo=on
#   both_low      PL=360W, SM=180MHz, CPU=18%, turbo=off
#   energy_opt    PL=250W, SM=1800MHz, CPU=100%, turbo=on (best energy efficiency)
#
# RTX 5080 supported values:
#   Power Limit:  150 - 450 W (default: 360W)
#   SM Clocks:    180, 600, 1200, 1800, 2400, 3090 MHz
#   Mem Clocks:   405, 810, 7001, 14801, 15001 MHz
#
# Intel Xeon w5-2455X CPU knobs:
#   max_perf_pct: 1-100 (percentage of max P-state, 18% ~ 800MHz)
#   no_turbo:     0 = turbo on, 1 = turbo off
#

set -euo pipefail

GPU_ID=1
DEFAULT_PL=360
INTEL_PSTATE="/sys/devices/system/cpu/intel_pstate"

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ── Helper Functions ──────────────────────────────────────────────────────────

info()  { echo -e "${CYAN}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }

require_sudo() {
    if [[ $EUID -ne 0 ]]; then
        # Try sudo
        if ! sudo -n true 2>/dev/null; then
            err "This command requires sudo. Run with sudo or configure NOPASSWD."
            exit 1
        fi
    fi
}

# ── GPU Knobs ─────────────────────────────────────────────────────────────────

# Knob 1: GPU Power Limit (nvidia-smi -pl)
set_gpu_power_limit() {
    local watts=$1
    require_sudo
    sudo nvidia-smi -i $GPU_ID -pl "$watts"
    ok "GPU power limit set to ${watts}W"
}

# Knob 2: GPU SM Clock (nvidia-smi --lock-gpu-clocks)
set_gpu_sm_clock() {
    local mhz=$1
    require_sudo
    sudo nvidia-smi -i $GPU_ID --lock-gpu-clocks="$mhz,$mhz"
    ok "GPU SM clock locked to ${mhz}MHz"
}

reset_gpu_sm_clock() {
    require_sudo
    sudo nvidia-smi -i $GPU_ID --reset-gpu-clocks
    ok "GPU SM clock unlocked"
}

# Knob 3: GPU Memory Clock (nvidia-smi --lock-memory-clocks)
set_gpu_mem_clock() {
    local mhz=$1
    require_sudo
    sudo nvidia-smi -i $GPU_ID --lock-memory-clocks="$mhz,$mhz"
    ok "GPU memory clock locked to ${mhz}MHz"
}

reset_gpu_mem_clock() {
    require_sudo
    sudo nvidia-smi -i $GPU_ID --reset-memory-clocks
    ok "GPU memory clock unlocked"
}

# ── CPU Knobs ─────────────────────────────────────────────────────────────────

# Knob 4: CPU max performance percentage (intel_pstate)
set_cpu_max_perf_pct() {
    local pct=$1
    require_sudo
    echo "$pct" | sudo tee "$INTEL_PSTATE/max_perf_pct" > /dev/null
    ok "CPU max_perf_pct set to ${pct}%"
}

# Knob 5: CPU turbo boost (intel_pstate)
set_cpu_turbo() {
    local no_turbo=$1  # 0=turbo on, 1=turbo off
    require_sudo
    echo "$no_turbo" | sudo tee "$INTEL_PSTATE/no_turbo" > /dev/null
    if [[ "$no_turbo" == "0" ]]; then
        ok "CPU turbo boost ENABLED"
    else
        ok "CPU turbo boost DISABLED"
    fi
}

# ── Status ────────────────────────────────────────────────────────────────────

show_status() {
    echo ""
    echo "===== GPU Settings (GPU index $GPU_ID) ====="

    # Power limit
    local pl
    pl=$(nvidia-smi -i $GPU_ID --query-gpu=power.limit --format=csv,noheader,nounits 2>/dev/null || echo "N/A")
    echo "  Power Limit:    ${pl}W"

    # Current clocks
    local sm_clk mem_clk
    sm_clk=$(nvidia-smi -i $GPU_ID --query-gpu=clocks.current.sm --format=csv,noheader,nounits 2>/dev/null || echo "N/A")
    mem_clk=$(nvidia-smi -i $GPU_ID --query-gpu=clocks.current.memory --format=csv,noheader,nounits 2>/dev/null || echo "N/A")
    echo "  SM Clock:       ${sm_clk} MHz (current)"
    echo "  Memory Clock:   ${mem_clk} MHz (current)"

    # Temperature & power draw
    local temp power_draw
    temp=$(nvidia-smi -i $GPU_ID --query-gpu=temperature.gpu --format=csv,noheader,nounits 2>/dev/null || echo "N/A")
    power_draw=$(nvidia-smi -i $GPU_ID --query-gpu=power.draw --format=csv,noheader,nounits 2>/dev/null || echo "N/A")
    echo "  Temperature:    ${temp}C"
    echo "  Power Draw:     ${power_draw}W"

    echo ""
    echo "===== CPU Settings ====="
    if [[ -f "$INTEL_PSTATE/max_perf_pct" ]]; then
        local cpu_pct cpu_turbo cpu_freq
        cpu_pct=$(cat "$INTEL_PSTATE/max_perf_pct")
        cpu_turbo=$(cat "$INTEL_PSTATE/no_turbo")
        cpu_freq=$(($(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq) / 1000))
        echo "  max_perf_pct:   ${cpu_pct}%"
        echo "  no_turbo:       ${cpu_turbo} (0=turbo on, 1=turbo off)"
        echo "  Current Freq:   ${cpu_freq} MHz (cpu0)"
    else
        echo "  intel_pstate not available"
    fi
    echo ""
}

# ── Presets ───────────────────────────────────────────────────────────────────

apply_preset() {
    local preset=$1
    info "Applying preset: $preset"

    case "$preset" in
        default)
            set_gpu_power_limit $DEFAULT_PL
            reset_gpu_sm_clock
            reset_gpu_mem_clock
            set_cpu_max_perf_pct 100
            set_cpu_turbo 0
            ;;
        low_power)
            set_gpu_power_limit 250
            set_gpu_sm_clock 1800
            reset_gpu_mem_clock
            set_cpu_max_perf_pct 100
            set_cpu_turbo 0
            ;;
        low_clock)
            set_gpu_power_limit $DEFAULT_PL
            set_gpu_sm_clock 600
            reset_gpu_mem_clock
            set_cpu_max_perf_pct 100
            set_cpu_turbo 0
            ;;
        high_perf)
            set_gpu_power_limit 450
            set_gpu_sm_clock 3090
            reset_gpu_mem_clock
            set_cpu_max_perf_pct 100
            set_cpu_turbo 0
            ;;
        cpu_low)
            set_gpu_power_limit $DEFAULT_PL
            reset_gpu_sm_clock
            reset_gpu_mem_clock
            set_cpu_max_perf_pct 18
            set_cpu_turbo 1
            ;;
        gpu_low)
            set_gpu_power_limit $DEFAULT_PL
            set_gpu_sm_clock 180
            reset_gpu_mem_clock
            set_cpu_max_perf_pct 100
            set_cpu_turbo 0
            ;;
        both_low)
            set_gpu_power_limit $DEFAULT_PL
            set_gpu_sm_clock 180
            reset_gpu_mem_clock
            set_cpu_max_perf_pct 18
            set_cpu_turbo 1
            ;;
        energy_opt)
            set_gpu_power_limit 250
            set_gpu_sm_clock 1800
            reset_gpu_mem_clock
            set_cpu_max_perf_pct 100
            set_cpu_turbo 0
            ;;
        *)
            err "Unknown preset: $preset"
            echo "Available presets: default, low_power, low_clock, high_perf, cpu_low, gpu_low, both_low, energy_opt"
            exit 1
            ;;
    esac

    ok "Preset '$preset' applied"
}

# ── Main ──────────────────────────────────────────────────────────────────────

usage() {
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  status                Show current GPU/CPU settings"
    echo "  set <preset>          Apply a preset (default|low_power|low_clock|high_perf|cpu_low|gpu_low|both_low|energy_opt)"
    echo "  reset                 Restore all defaults"
    echo "  set-gpu-pl <watts>    Set GPU power limit (150-450W)"
    echo "  set-gpu-clk <mhz>    Lock GPU SM clock (180/600/1200/1800/2400/3090)"
    echo "  set-gpu-mem <mhz>    Lock GPU memory clock (405/810/7001/14801/15001)"
    echo "  set-cpu-pct <pct>    Set CPU max_perf_pct (1-100)"
    echo "  set-cpu-turbo <0|1>  Set CPU turbo (0=on, 1=off)"
    echo ""
    echo "Knobs summary:"
    echo "  1. GPU Power Limit    nvidia-smi -i 1 -pl <W>"
    echo "  2. GPU SM Clock       nvidia-smi -i 1 --lock-gpu-clocks=<MHz>,<MHz>"
    echo "  3. GPU Memory Clock   nvidia-smi -i 1 --lock-memory-clocks=<MHz>,<MHz>"
    echo "  4. CPU max_perf_pct   /sys/devices/system/cpu/intel_pstate/max_perf_pct"
    echo "  5. CPU Turbo Boost    /sys/devices/system/cpu/intel_pstate/no_turbo"
}

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

case "$1" in
    status)
        show_status
        ;;
    set)
        [[ $# -lt 2 ]] && { err "Missing preset name"; usage; exit 1; }
        apply_preset "$2"
        show_status
        ;;
    reset)
        apply_preset "default"
        show_status
        ;;
    set-gpu-pl)
        [[ $# -lt 2 ]] && { err "Missing watts value"; exit 1; }
        set_gpu_power_limit "$2"
        ;;
    set-gpu-clk)
        [[ $# -lt 2 ]] && { err "Missing MHz value"; exit 1; }
        set_gpu_sm_clock "$2"
        ;;
    set-gpu-mem)
        [[ $# -lt 2 ]] && { err "Missing MHz value"; exit 1; }
        set_gpu_mem_clock "$2"
        ;;
    set-cpu-pct)
        [[ $# -lt 2 ]] && { err "Missing percentage value"; exit 1; }
        set_cpu_max_perf_pct "$2"
        ;;
    set-cpu-turbo)
        [[ $# -lt 2 ]] && { err "Missing value (0 or 1)"; exit 1; }
        set_cpu_turbo "$2"
        ;;
    *)
        err "Unknown command: $1"
        usage
        exit 1
        ;;
esac
