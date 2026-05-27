#!/usr/bin/env bash
# run.sh — entrypoint for running the de2-controller container with your config.
#
# Usage:
#   ./run.sh deploy    # provision and start the cluster
#   ./run.sh collect   # snapshot results to ./results/
#   ./run.sh bash      # interactive shell inside the controller (debugging)

set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
    echo "ERROR: .env not found. Copy .env.example to .env and fill in your paths."
    exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${SSH_KEY_PATH:?SSH_KEY_PATH not set in .env}"
: "${OPENSTACK_KEY_NAME:?OPENSTACK_KEY_NAME not set in .env}"
: "${OPENRC_PATH:?OPENRC_PATH not set in .env}"
: "${GITHUB_TOKENS_PATH:?GITHUB_TOKENS_PATH not set in .env}"

[[ -f "$SSH_KEY_PATH" ]]       || { echo "ERROR: SSH key not found at $SSH_KEY_PATH"; exit 1; }
[[ -f "$OPENRC_PATH" ]]        || { echo "ERROR: openrc.sh not found at $OPENRC_PATH"; exit 1; }
[[ -f "$GITHUB_TOKENS_PATH" ]] || { echo "ERROR: tokens file not found at $GITHUB_TOKENS_PATH"; exit 1; }

CMD="${1:-deploy}"
case "$CMD" in
    deploy)  ENTRY=(./deploy.sh) ;;
    collect) ENTRY=(./collect.sh) ;;
    bash)    ENTRY=(bash) ;;
    *)       echo "Usage: $0 {deploy|collect|bash}"; exit 1 ;;
esac

mkdir -p results state

docker run --rm -it \
    -v "$(realpath "$SSH_KEY_PATH"):/root/.ssh/key.pem:ro" \
    -v "$(realpath "$OPENRC_PATH"):/openrc.sh:ro" \
    -v "$(realpath "$GITHUB_TOKENS_PATH"):/tokens/.github_tokens:ro" \
    -v "$(pwd)/results:/results" \
    -v "$(pwd)/state:/controller/state" \
    -e SSH_KEY=/root/.ssh/key.pem \
    -e KEY_NAME="$OPENSTACK_KEY_NAME" \
    -e DAYS_BACK="${DAYS_BACK:-7}" \
    theodorafc02/de2-controller:latest \
    "${ENTRY[@]}"
