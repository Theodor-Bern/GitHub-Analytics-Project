# syntax=docker/dockerfile:1.6
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


FROM python:3.11-slim AS runtime

WORKDIR /app

COPY --from=builder /venv /venv
ENV PATH="/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

COPY language_aggregator.py .
COPY commit_aggregator.py   .
COPY test_aggregator.py     .
COPY ci_aggregator.py       .

# No CMD on purpose — compose must specify which aggregator to run.
