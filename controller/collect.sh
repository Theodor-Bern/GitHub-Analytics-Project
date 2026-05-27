#!/usr/bin/env bash
# collect.sh — snapshot logs and results from the aggregator VM.
#
# Run AFTER deploy.sh. Repeatable: each call writes a fresh timestamped
# snapshot under /results/ inside the container.

set -euo pipefail

SSH_KEY="${SSH_KEY:?SSH_KEY env var must point to the mounted private key (see README)}"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_OPTS=(-i "$SSH_KEY"
          -o StrictHostKeyChecking=no
          -o UserKnownHostsFile=/dev/null
          -o LogLevel=ERROR)

INVENTORY="${INVENTORY:-/controller/state/inventory.env}"
RESULTS_DIR="${RESULTS_DIR:-/results}"

log()  { echo -e "\033[1;34m[collect]\033[0m $*"; }
die()  { echo -e "\033[1;31m[collect]\033[0m $*"; exit 1; }

[[ -f "$SSH_KEY" ]]   || die "SSH key not found at $SSH_KEY"
[[ -f "$INVENTORY" ]] || die "Inventory not found at $INVENTORY — has deploy.sh run?"

# shellcheck disable=SC1090
source "$INVENTORY"
[[ -n "${AGGREGATOR_IP:-}" ]] || die "AGGREGATOR_IP missing from inventory"

mkdir -p "$RESULTS_DIR"
timestamp=$(date +"%Y%m%d-%H%M%S")
snapshot_dir="$RESULTS_DIR/snapshot-$timestamp"
mkdir -p "$snapshot_dir"
log "Snapshot directory: $snapshot_dir"

containers=(
    language-aggregator
    commit-aggregator
    test-aggregator
    ci-aggregator
)

for c in "${containers[@]}"; do
    out="$snapshot_dir/$c.log"
    log "  fetching $c → $out"
    ssh "${SSH_OPTS[@]}" "$SSH_USER@$AGGREGATOR_IP" \
        "sudo docker logs $c 2>&1" > "$out" || {
        log "    (warning) failed to fetch $c"
    }
done

# Extract the last "── Top N ──" block from each log into a quick-look summary.
summary="$snapshot_dir/SUMMARY.txt"
{
    echo "Snapshot $timestamp"
    echo ""
    for c in "${containers[@]}"; do
        echo "==================================================================="
        echo "  $c — latest top-N"
        echo "==================================================================="
        awk '/── Top/{block=""} {block=block $0 "\n"} END{printf "%s", block}' \
            "$snapshot_dir/$c.log" || echo "(no output)"
        echo ""
    done
} > "$summary"

log "Fetching JSON result files..."

json_files=(
    "results_q1.json"
    "results_q2.json"
    "results_q3.json"
    "results_q4.json"
)

for f in "${json_files[@]}"; do
    out="$snapshot_dir/$f"
    log "  fetching $f → $out"
    ssh "${SSH_OPTS[@]}" "$SSH_USER@$AGGREGATOR_IP" \
        "cat /home/ubuntu/aggregator/$f 2>/dev/null" > "$out" || {
        log "    (warning) $f not found — has the aggregator been stopped yet?"
    }
done

log "Merging JSON files into results.json..."

python3 - <<PYEOF
import json, os

snapshot = "$snapshot_dir"
files = {
    "q1_top_languages":    "results_q1.json",
    "q2_top_commits":      "results_q2.json",
    "q3_tdd_languages":    "results_q3.json",
    "q4_devops_languages": "results_q4.json",
}

merged = {}
for key, fname in files.items():
    path = os.path.join(snapshot, fname)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path) as f:
            data = json.load(f)
        merged[key] = data.get(key, [])
    else:
        print(f"  (warning) {fname} missing or empty, skipping {key}")
        merged[key] = []

out_path = os.path.join(snapshot, "results.json")
with open(out_path, "w") as f:
    json.dump(merged, f, indent=2)

print(f"  results.json written to {out_path}")
PYEOF

if [[ -f "$snapshot_dir/results.json" ]]; then
    log "Generating plots..."
    RESULTS_FILE="$snapshot_dir/results.json" \
    FIGURES_DIR="$snapshot_dir/figures" \
    python3 /controller/plot.py && log "Plots saved to $snapshot_dir/figures/"
else
    log "(warning) results.json not found, skipping plots"
fi

log "Done."
log "  Logs:    $snapshot_dir/"
log "  Summary: $summary"
log "  Results: $snapshot_dir/results.json"
log "  Figures: $snapshot_dir/figures/"
