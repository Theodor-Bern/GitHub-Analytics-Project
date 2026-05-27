#!/usr/bin/env python3
"""
Producer: paginates GitHub's Search API one day at a time and publishes
each repo to the `repos.raw` Pulsar topic. After every publish, blocks
until DONE events arrive on the control topic from all three downstream
stages (language, commits, tests).
"""
import os
import sys
import json
import time
import datetime as dt
import uuid
import requests
import pulsar

PULSAR_URL    = os.environ["PULSAR_URL"]
GITHUB_TOKEN  = os.environ["GITHUB_TOKEN"]
DAYS_BACK     = int(os.environ.get("DAYS_BACK", "365"))
TOPIC         = os.environ.get("TOPIC", "repos.raw")
CONTROL_TOPIC = os.environ.get("CONTROL_TOPIC", "repos.raw.control")
PER_PAGE      = 100  # GitHub Search API max
MAX_PAGES     = 10   # GitHub caps Search results at the first 1000 matches

# Unique id per run. Consumers copy it into DONE events so we can ignore
# DONEs left over from previous producer runs on the same control topic.
RUN_ID        = os.environ.get("RUN_ID", str(uuid.uuid4()))
DONE_TIMEOUT_SECONDS = int(os.environ.get("DONE_TIMEOUT_SECONDS", "5400"))
REQUIRED_STAGES = {"language", "commits", "tests"}

GITHUB_API    = "https://api.github.com/search/repositories"

HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "X-GitHub-Api-Version": "2022-11-28",
}

KEEP_FIELDS = ["id", "full_name", "language", "created_at", "pushed_at",
               "default_branch", "stargazers_count", "forks_count"]

TRANSIENT_STATUSES = {500, 502, 503, 504}
RETRY_ATTEMPTS = 5
RETRY_BASE_SECONDS = 2


def keep(repo):
    trimmed = {k: repo.get(k) for k in KEEP_FIELDS}
    trimmed["owner_login"] = (repo.get("owner") or {}).get("login")
    return trimmed


def respect_rate_limit(resp):
    """Sleep until the Search API quota resets when we're near the limit."""
    remaining = int(resp.headers.get("X-RateLimit-Remaining", 1))
    reset_ts  = int(resp.headers.get("X-RateLimit-Reset", time.time()))
    if remaining <= 2:
        wait = max(0, reset_ts - int(time.time())) + 2
        print(f"  [rate-limit] {remaining} calls left, sleeping {wait}s until reset", flush=True)
        time.sleep(wait)


def _get_with_retry(url, **kwargs):
    """GET with bounded exponential backoff on transient network/5xx errors."""
    last_exc = None
    last_status = None

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            if attempt == RETRY_ATTEMPTS:
                break
            wait = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            print(
                f"  [retry] {type(exc).__name__} on attempt "
                f"{attempt}/{RETRY_ATTEMPTS}, sleeping {wait}s",
                flush=True,
            )
            time.sleep(wait)
            continue

        if resp.status_code in TRANSIENT_STATUSES:
            last_status = resp.status_code
            if attempt == RETRY_ATTEMPTS:
                break
            wait = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            print(
                f"  [retry] HTTP {resp.status_code} on attempt "
                f"{attempt}/{RETRY_ATTEMPTS}, sleeping {wait}s",
                flush=True,
            )
            time.sleep(wait)
            continue

        return resp

    if last_exc:
        raise last_exc
    raise RuntimeError(f"GET {url} kept returning transient HTTP status {last_status}")


def fetch_day(date_str):
    """Yield trimmed repo dicts for all repos created on `date_str`."""
    query = f"created:{date_str}..{date_str}"
    for page in range(1, MAX_PAGES + 1):
        params = {
            "q": query,
            "per_page": PER_PAGE,
            "page": page,
            "sort": "stars",
            "order": "desc",
        }

        resp = _get_with_retry(GITHUB_API, headers=HEADERS, params=params, timeout=30)

        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            respect_rate_limit(resp)
            resp = _get_with_retry(GITHUB_API, headers=HEADERS, params=params, timeout=30)

        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])

        if not items:
            return

        for repo in items:
            yield keep(repo)

        if len(items) < PER_PAGE:
            return

        respect_rate_limit(resp)


def date_range(days_back):
    """Yield ISO date strings from `days_back` ago up to (but not including) today."""
    today = dt.date.today()
    for n in range(days_back, 0, -1):
        yield (today - dt.timedelta(days=n)).isoformat()


def wait_for_all_done(control_consumer, job_id, repo_id, timeout_seconds):
    """Block until language, commits, and tests have sent DONE for this job_id."""
    seen = set()
    deadline = time.time() + timeout_seconds

    while seen != REQUIRED_STAGES:
        if time.time() > deadline:
            missing = REQUIRED_STAGES - seen
            raise TimeoutError(
                f"Timed out waiting for repo_id={repo_id}, job_id={job_id}. Missing={sorted(missing)}"
            )

        try:
            msg = control_consumer.receive(timeout_millis=10_000)
        except pulsar.Timeout:
            print(
                f"  waiting for DONE repo_id={repo_id}: "
                f"seen={sorted(seen)} missing={sorted(REQUIRED_STAGES - seen)}",
                flush=True,
            )
            continue

        try:
            event = json.loads(msg.data().decode("utf-8"))
            control_consumer.acknowledge(msg)

            if event.get("type") != "DONE":
                continue
            if event.get("run_id") != RUN_ID:
                continue
            if event.get("job_id") != job_id:
                continue

            stage = event.get("stage")
            if stage in REQUIRED_STAGES:
                seen.add(stage)
                status = event.get("status", "ok")
                print(
                    f"  DONE repo_id={repo_id} stage={stage} status={status} "
                    f"({len(seen)}/{len(REQUIRED_STAGES)})",
                    flush=True,
                )

        except Exception as e:
            print(f"  error while reading control event: {e}", flush=True)
            control_consumer.negative_acknowledge(msg)


def main():
    print(f"Connecting to Pulsar at {PULSAR_URL}", flush=True)
    print(f"RUN_ID={RUN_ID}", flush=True)

    client   = pulsar.Client(PULSAR_URL)

    producer = client.create_producer(
        TOPIC,
        max_pending_messages=100,
        block_if_queue_full=True,
    )

    # Unique subscription so this producer only sees DONEs from its own run.
    control_consumer = client.subscribe(
        CONTROL_TOPIC,
        subscription_name=f"producer-control-{RUN_ID}",
        initial_position=pulsar.InitialPosition.Latest,
        consumer_type=pulsar.ConsumerType.Exclusive,
    )

    print(f"Producing to topic '{TOPIC}', scanning {DAYS_BACK} days of history", flush=True)
    print(f"Waiting for DONE events on '{CONTROL_TOPIC}'", flush=True)

    total = 0
    try:
        for date_str in date_range(DAYS_BACK):
            print(f"\n{date_str}", flush=True)
            day_count = 0

            for repo in fetch_day(date_str):
                repo_id = str(repo["id"])
                job_id = f"{RUN_ID}:{repo_id}"

                repo["run_id"] = RUN_ID
                repo["job_id"] = job_id

                payload = json.dumps(repo).encode("utf-8")
                producer.send(
                    payload,
                    properties={
                        "repo_id": repo_id,
                        "job_id": job_id,
                        "run_id": RUN_ID,
                        "fetched_on": date_str,
                    },
                )

                print(f"  published repo_id={repo_id} {repo.get('full_name')}; waiting for 3 DONEs", flush=True)
                wait_for_all_done(control_consumer, job_id, repo_id, DONE_TIMEOUT_SECONDS)

                day_count += 1
                total += 1
                print(f"  repo_id={repo_id} fully processed; total={total}", flush=True)

            print(f"  → completed {day_count} repos for {date_str}  (total: {total})", flush=True)

        print(f"\nDone. Total repos fully processed: {total}", flush=True)

    except KeyboardInterrupt:
        print(f"\nStopped. Total repos fully processed: {total}", flush=True)
    finally:
        control_consumer.close()
        producer.close()
        client.close()


if __name__ == "__main__":
    main()
