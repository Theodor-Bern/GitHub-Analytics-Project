#!/usr/bin/env python3
import os
import re
import sys
import time
import random
from os import environ as env

from novaclient import client
from keystoneauth1 import loading, session

KEY_NAME        = env.get("KEY_NAME") or sys.exit(
    "ERROR: set KEY_NAME"
)
FLAVOR = env.get("FLAVOR", "ssc.medium")
IMAGE_NAME = env.get("IMAGE_NAME", "Ubuntu 22.04 - 2024.01.15")
PRIVATE_NET = env.get("PRIVATE_NET", "UPPMAX 2026/1-24 Internal IPv4 Network")
SECURITY_GROUPS = ["default"]
CLOUD_INIT_DIR  = env.get("CLOUD_INIT_DIR", "/controller/cloud-init")
INVENTORY_PATH  = env.get("INVENTORY_PATH", "/controller/state/inventory.env")

identifier = random.randint(1000, 9999)

loader = loading.get_plugin_loader('password')
auth = loader.load_from_options(
    auth_url = env['OS_AUTH_URL'],
    username = env['OS_USERNAME'],
    password = env['OS_PASSWORD'],
    project_name = env['OS_PROJECT_NAME'],
    project_domain_id = env['OS_PROJECT_DOMAIN_ID'],
    user_domain_name = env['OS_USER_DOMAIN_NAME'],
)
nova = client.Client('2.1', session=session.Session(auth=auth))

image = nova.glance.find_image(IMAGE_NAME)
flavor = nova.flavors.find(name=FLAVOR)
net = nova.neutron.find_network(PRIVATE_NET)
nics = [{'net-id': net.id}]


def read_cfg(filename):
    path = os.path.join(CLOUD_INIT_DIR, filename)
    if not os.path.isfile(path):
        sys.exit(f"missing {path}")
    with open(path) as f:
        return f.read()


def launch(name, userdata):
    return nova.servers.create(
        name = f"{name}-{identifier}",
        image = image,
        flavor = flavor,
        key_name = KEY_NAME,
        userdata = userdata,
        nics = nics,
        security_groups= SECURITY_GROUPS,
    )


def get_ip(instance):
    nets = instance.networks or {}
    addrs = nets.get(PRIVATE_NET, [])
    for n in addrs:
        if re.match(r'\d+\.\d+\.\d+\.\d+', n):
            return n
    return None


def wait_ip(instance):
    while True:
        updated = nova.servers.get(instance.id)
        ip = get_ip(updated)
        if ip:
            return ip, updated
        time.sleep(5)


def wait_active(instance, name):
    while True:
        updated = nova.servers.get(instance.id)
        if updated.status == 'ACTIVE':
            return updated
        if updated.status == 'ERROR':
            sys.exit(f"{name} is error")
        time.sleep(5)


print("launching broker")
broker = launch("broker-vm", read_cfg("broker.yaml"))
broker_ip, broker = wait_ip(broker)
print(f"broker ip {broker_ip}")

print("launching the rest")
cfg_producer = read_cfg("producer.yaml").replace("{{BROKER_IP}}", broker_ip)
cfg_consumer = read_cfg("consumer.yaml").replace("{{BROKER_IP}}", broker_ip)
cfg_aggregator = read_cfg("aggregator.yaml").replace("{{BROKER_IP}}", broker_ip)

producer = launch("producer-vm", cfg_producer)
consumer = launch("consumer-vm", cfg_consumer)
aggregator = launch("aggregator-vm", cfg_aggregator)

instances = {
    "broker":     broker,
    "producer":   producer,
    "consumer":   consumer,
    "aggregator": aggregator,
}

time.sleep(10)

for role, inst in instances.items():
    inst = wait_active(inst, role)
    _, inst = wait_ip(inst)
    instances[role] = inst

ips = {}
for role, inst in instances.items():
    ip = get_ip(inst)
    ips[role] = ip
    print(f"{role}: {ip}")

os.makedirs(os.path.dirname(INVENTORY_PATH), exist_ok=True)
with open(INVENTORY_PATH, "w") as f:
    f.write(f"BROKER_IP={ips['broker']}\n")
    f.write(f"PRODUCER_IP={ips['producer']}\n")
    f.write(f"CONSUMER_IP={ips['consumer']}\n")
    f.write(f"AGGREGATOR_IP={ips['aggregator']}\n")

print(f"\nInventory with IP addresses at {INVENTORY_PATH}")
print(f"Broker reachable at: pulsar://{ips['broker']}:6650")
print("All VMs active")
