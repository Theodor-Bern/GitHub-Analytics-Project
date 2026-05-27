#!/usr/bin/env python3

import os
import re
import json
import time
import requests
import pulsar

from token_pool import TokenPool

PULSAR_URL    = os.environ["PULSAR_URL"]
GITHUB_TOKENS = os.environ["GITHUB_TOKENS"].split(",")
INPUT_TOPIC   = os.environ.get("INPUT_TOPIC",  "repos.raw")
OUTPUT_TOPIC  = os.environ.get("OUTPUT_TOPIC", "commit-topic")
CONTROL_TOPIC = os.environ.get("CONTROL_TOPIC", "repos.raw.control")
SUBSCRIPTION  = os.environ.get("SUBSCRIPTION", "commits-enricher-sub")
LOG_EVERY     = int(os.environ.get("LOG_EVERY", "50"))

LAST_PAGE_RE = re.compile(r'<[^>]*[?&]page=(\d+)[^>]*>;\s*rel="last"')


def count_commits(pool, owner_login, repo_name):
    """
    Return total commits on the default branch, or None if undeterminable.

    409: empty repo (0 commits). 403/404: gone or inaccessible (skip).
    5xx after pool retries: skip instead of nack-looping forever.
    """
    url = f"https://api.github.com/repos/{owner_login}/{repo_name}/commits"
    resp = pool.get(url, params={"per_page": 1}, timeout=30)

    if resp.status_code == 409:
        return 0
    if resp.status_code in (403, 404):
        return None
    if resp.status_code >= 500:
        print(f"  GitHub {resp.status_code} on {url}, skipping", flush=True)
        return None

    resp.raise_for_status()

    link = resp.headers.get("Link", "")
    match = LAST_PAGE_RE.search(link)
    if match:
        return int(match.group(1))

    # No 'last' link → either 0 or 1 commits
    items = resp.json()
    return len(items) if isinstance(items, list) else 0


def send_done(control_producer, repo, stage, status="ok", error=None):
    event = {
        "type": "DONE",
        "run_id": repo.get("run_id"),
        "job_id": repo.get("job_id"),
        "repo_id": str(repo.get("id")),
        "full_name": repo.get("full_name"),
        "stage": stage,
        "status": status,
        "error": str(error) if error else None,
        "ts": time.time(),
    }
    control_producer.send(
        json.dumps(event).encode("utf-8"),
        properties={
            "type": "DONE",
            "stage": stage,
            "status": status,
            "run_id": str(repo.get("run_id")),
            "job_id": str(repo.get("job_id")),
            "repo_id": str(repo.get("id")),
        },
    )


def main():
    pool = TokenPool(GITHUB_TOKENS)
    print(f"Connecting to Pulsar at {PULSAR_URL}", flush=True)
    print(f"GitHub PAT pool: {len(pool.tokens)} token(s)", flush=True)
    client = pulsar.Client(PULSAR_URL)

    consumer = client.subscribe(
        INPUT_TOPIC,
        subscription_name=SUBSCRIPTION,
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest,
        receiver_queue_size=1,
    )

    producer = client.create_producer(OUTPUT_TOPIC)
    control_producer = client.create_producer(CONTROL_TOPIC)

    print(f"Subscribed to '{INPUT_TOPIC}' → producing to '{OUTPUT_TOPIC}'", flush=True)
    print(f"Sending DONE events to '{CONTROL_TOPIC}'", flush=True)

    processed = 0
    skipped = 0
    try:
        while True:
            msg = consumer.receive()
            try:
                repo = json.loads(msg.data().decode("utf-8"))
                owner = repo.get("owner_login")
                name = repo.get("full_name", "").split("/")[-1]

                if not owner or not name:
                    send_done(control_producer, repo, stage="commits", status="skipped")
                    consumer.acknowledge(msg)
                    skipped += 1
                    continue

                commits = count_commits(pool, owner, name)
                if commits is None:
                    send_done(control_producer, repo, stage="commits", status="skipped")
                    consumer.acknowledge(msg)
                    skipped += 1
                    continue

                enriched = {
                    "repo_id":      repo.get("id"),
                    "full_name":    repo.get("full_name"),
                    "language":     repo.get("language"),
                    "commit_count": commits,
                }

                # Publish result first, then DONE, then ack input. If we crash
                # between publish and DONE, the message is redelivered and the
                # producer eventually gets its DONE — at-least-once semantics.
                producer.send(
                    json.dumps(enriched).encode("utf-8"),
                    properties={"repo_id": str(repo.get("id"))},
                )

                send_done(control_producer, repo, stage="commits", status="ok")
                consumer.acknowledge(msg)

                processed += 1

                if (processed + skipped) % LOG_EVERY == 0:
                    print(f"  processed={processed}  skipped={skipped}  "
                          f"latest={repo.get('full_name')} ({commits} commits)", flush=True)

            except requests.exceptions.RequestException as e:
                print(f"  network error: {e}", flush=True)
                consumer.negative_acknowledge(msg)
            except Exception as e:
                print(f"  unexpected error: {e}", flush=True)
                consumer.negative_acknowledge(msg)

    except KeyboardInterrupt:
        print(f"\nFinal: processed={processed}, skipped={skipped}", flush=True)
    finally:
        control_producer.close()
        producer.close()
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
