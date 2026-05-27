#!/usr/bin/env bash
set -euo pipefail

SSH_KEY="${SSH_KEY:?SSH_KEY env var must point to the mounted private key (see README)}"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_OPTS=(-i "$SSH_KEY"
          -o StrictHostKeyChecking=no
          -o UserKnownHostsFile=/dev/null
          -o LogLevel=ERROR)

OPENRC="${OPENRC:-/openrc.sh}"
SECRETS_FILE="${SECRETS_FILE:-/secrets/.openstack-env}"
TOKENS_FILE="${TOKENS_FILE:-/tokens/.github_tokens}"
INVENTORY="${INVENTORY:-/controller/state/inventory.env}"
COMPOSE_DIR="/controller/compose-files"
CLOUD_INIT_WAIT_SECS="${CLOUD_INIT_WAIT_SECS:-600}"
BROKER_WAIT_SECS="${BROKER_WAIT_SECS:-180}"

[[ -f "$OPENRC" ]]      || { echo "missing $OPENRC"; exit 1; }
[[ -f "$SSH_KEY" ]]     || { echo "missing $SSH_KEY"; exit 1; }
[[ -f "$TOKENS_FILE" ]] || { echo "missing $TOKENS_FILE"; exit 1; }

# SSH refuses keys that aren't 600/400. Read-only mounts can't be chmod'd here.
perms=$(stat -c '%a' "$SSH_KEY" 2>/dev/null || echo "")
if [[ -n "$perms" && "$perms" != "600" && "$perms" != "400" ]]; then
    echo "warning: $SSH_KEY has permissions $perms, ssh may refuse it"
fi

source "$OPENRC"
if [[ -f "$SECRETS_FILE" ]]; then
    set -a; source "$SECRETS_FILE"; set +a
fi

echo "running start_instances.py"
python3 /controller/start_instances.py

[[ -f "$INVENTORY" ]] || { echo "no inventory written"; exit 1; }
source "$INVENTORY"
echo "broker=$BROKER_IP producer=$PRODUCER_IP consumer=$CONSUMER_IP aggregator=$AGGREGATOR_IP"

wait_for_cloud_init() {
    local ip=$1 role=$2 deadline=$(( $(date +%s) + CLOUD_INIT_WAIT_SECS ))
    echo "waiting for cloud-init on $role"
    while (( $(date +%s) < deadline )); do
        if ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "test -f /home/ubuntu/.cloud-init-done" 2>/dev/null; then
            return 0
        fi
        sleep 10
    done
    echo "$role timed out"
    exit 1
}

wait_for_cloud_init "$BROKER_IP" "broker"
wait_for_cloud_init "$AGGREGATOR_IP" "aggregator"
wait_for_cloud_init "$CONSUMER_IP" "consumer"
wait_for_cloud_init "$PRODUCER_IP" "producer"

distribute() {
    local ip=$1 role=$2 compose_subdir=$3 needs_tokens=$4
    echo "disributing to $role"
    ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "mkdir -p /home/ubuntu/$role"
    scp "${SSH_OPTS[@]}" "$COMPOSE_DIR/$compose_subdir/docker-compose.yml" \
        "$SSH_USER@$ip:/home/ubuntu/$role/docker-compose.yml"
    if [[ "$needs_tokens" == "yes" ]]; then
        scp "${SSH_OPTS[@]}" "$TOKENS_FILE" \
            "$SSH_USER@$ip:/home/ubuntu/$role/.env"
        ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "chmod 600 /home/ubuntu/$role/.env"
    fi
}

distribute "$BROKER_IP" broker broker no
distribute "$AGGREGATOR_IP" aggregator aggregators no
distribute "$CONSUMER_IP" consumer enrichers yes
distribute "$PRODUCER_IP" producer producer yes

if [[ -n "${DAYS_BACK:-}" ]]; then
    ssh "${SSH_OPTS[@]}" "$SSH_USER@$PRODUCER_IP" \
        "echo 'DAYS_BACK=$DAYS_BACK' >> /home/ubuntu/producer/.env"
fi

start_service() {
    local ip=$1 role=$2
    echo "starting $role"
    ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" \
        "cd /home/ubuntu/$role && sudo docker compose up -d"
}

start_service "$BROKER_IP" broker

echo "waiting for pulsar"
deadline=$(( $(date +%s) + BROKER_WAIT_SECS ))
while (( $(date +%s) < deadline )); do
    if ssh "${SSH_OPTS[@]}" "$SSH_USER@$BROKER_IP" \
         "nc -z localhost 6650" 2>/dev/null; then
        break
    fi
    sleep 5
done
ssh "${SSH_OPTS[@]}" "$SSH_USER@$BROKER_IP" "nc -z localhost 6650" 2>/dev/null \
    || { echo "pulsar didnt come up"; exit 1; }

# consumers subscribe first
start_service "$AGGREGATOR_IP" aggregator
start_service "$CONSUMER_IP" consumer
sleep 5
start_service "$PRODUCER_IP" producer

echo "Broker: $BROKER_IP"
echo "Aggregator: $AGGREGATOR_IP"
echo "Consumer: $CONSUMER_IP "
echo "Producer: $PRODUCER_IP "
