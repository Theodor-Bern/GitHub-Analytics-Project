# syntax=docker/dockerfile:1.6
#
# de2-controller — bootstraps and manages the 4-VM analytics cluster.
#
# Build (from repo root):
#   docker build -f controller/Dockerfile -t theodorafc02/de2-controller:latest .
#
# Run via the wrapper in the repo root:
#   ./run.sh deploy
#   ./run.sh collect

FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        openssh-client \
        netcat-openbsd \
        curl \
        ca-certificates \
        gawk \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        python-novaclient==18.6.0 \
        python-keystoneclient==5.4.0 \
        keystoneauth1==5.5.0 \
        matplotlib==3.9.2

WORKDIR /controller

COPY controller/start_instances.py  ./start_instances.py
COPY controller/deploy.sh           ./deploy.sh
COPY controller/collect.sh          ./collect.sh
COPY controller/plot.py             ./plot.py
COPY controller/cloud-init/         ./cloud-init/

COPY services/broker/docker-compose.yml      ./compose-files/broker/docker-compose.yml
COPY services/producer/docker-compose.yml    ./compose-files/producer/docker-compose.yml
COPY services/enrichers/docker-compose.yml   ./compose-files/enrichers/docker-compose.yml
COPY services/aggregators/docker-compose.yml ./compose-files/aggregators/docker-compose.yml

RUN chmod +x deploy.sh collect.sh

ENTRYPOINT []
CMD ["./deploy.sh"]
