#!/usr/bin/env python3

import os
import re
import json
import time
import requests
import pulsar

from token_pool import TokenPool

PULSAR_URL          = os.environ["PULSAR_URL"]
GITHUB_TOKENS       = os.environ["GITHUB_TOKENS"].split(",")
INPUT_TOPIC         = os.environ.get("INPUT_TOPIC",         "repos.raw")
TEST_TOPIC          = os.environ.get("TEST_TOPIC",          "test-topic")
TEST_AND_CI_TOPIC   = os.environ.get("TEST_AND_CI_TOPIC",   "test-and-ci-topic")
CONTROL_TOPIC       = os.environ.get("CONTROL_TOPIC",       "repos.raw.control")
SUBSCRIPTION        = os.environ.get("SUBSCRIPTION",        "test-enricher-sub")
LOG_EVERY           = int(os.environ.get("LOG_EVERY", "50"))

TEST_DIR_PATTERNS = [
    re.compile(r"(^|/)(tests?|__tests__|specs?)(/|$)", re.IGNORECASE),
]

TEST_FILE_PATTERNS = [
    re.compile(r".*[._]test[._].*\.(py|js|ts|tsx|jsx|go|rs|rb|java|kt)$",  re.IGNORECASE),
    re.compile(r".*[._]spec[._].*\.(js|ts|tsx|jsx|rb)$",                    re.IGNORECASE),
    re.compile(r"^test_.*\.py$",                                            re.IGNORECASE),
    re.compile(r".*_test\.go$",                                             re.IGNORECASE),
    re.compile(r".*Test\.java$"),
    re.compile(r".*Tests\.cs$"),
]

CI_FILE_PATTERNS = [
    re.compile(r"^\.github/workflows/.*\.ya?ml$",  re.IGNORECASE),
    re.compile(r"^\.travis\.ya?ml$",               re.IGNORECASE),
    re.compile(r"^\.circleci/config\.ya?ml$",      re.IGNORECASE),
    re.compile(r"^\.gitlab-ci\.ya?ml$",            re.IGNORECASE),
    re.compile(r"^Jenkinsfile$"),
    re.compile(r"^azure-pipelines\.ya?ml$",        re.IGNORECASE),
    re.compile(r"^bitbucket-pipelines\.ya?ml$",    re.IGNORECASE),
]


def has_test(path):
    for pat in TEST_DIR_PATTERNS:
        if pat.search(path):
            return True
    for pat in TEST_FILE_PATTERNS:
        if pat.search(path):
            return True
    return False


def has_ci(path):
    return any(pat.search(path) for pat in CI_FILE_PATTERNS)


def classify_tree(tree_entries):
    """Walk all paths once. Return (found_test, found_ci)."""
    found_test = False
    found_ci = False
    for entry in tree_entries:
        path = entry.get("path", "")
        if not found_test and has_test(path):
            found_test = True
        if not found_ci and has_ci(path):
            found_ci = True
        if found_test and found_ci:
            break
    return found_test, found_ci


def fetch_tree(pool, owner_login, repo_name, default_branch):
    """
    Return the recursive git tree, or None for inaccessible/empty/gone repos.
    Raises on transient network errors so the caller can nack.
    """
    if not default_branch:
        return None
    url = f"https://api.github.com/repos/{owner_login}/{repo_name}/git/trees/{default_branch}"
    resp = pool.get(url, params={"recursive": "1"}, timeout=30)

    if resp.status_code in (403, 404, 409):
        return None
    if resp.status_code >= 500:
        print(f"  GitHub {resp.status_code} on {url}, skipping", flush=True)
        return None

    resp.raise_for_status()
    return resp.json().get("tree", [])


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

    test_producer    = client.create_producer(TEST_TOPIC)
    test_ci_producer = client.create_producer(TEST_AND_CI_TOPIC)
    control_producer = client.create_producer(CONTROL_TOPIC)

    print(f"Subscribed to '{INPUT_TOPIC}'", flush=True)
    print(f"Publishing to '{TEST_TOPIC}' and '{TEST_AND_CI_TOPIC}'", flush=True)
    print(f"Sending DONE events to '{CONTROL_TOPIC}'", flush=True)

    stats = {"total": 0, "skipped": 0, "test_only": 0, "test_and_ci": 0, "neither": 0, "ci_only": 0}

    try:
        while True:
            msg = consumer.receive()
            try:
                repo = json.loads(msg.data().decode("utf-8"))
                owner  = repo.get("owner_login")
                name   = (repo.get("full_name") or "").split("/")[-1]
                branch = repo.get("default_branch")

                if not owner or not name:
                    send_done(control_producer, repo, stage="tests", status="skipped")
                    consumer.acknowledge(msg)
                    stats["skipped"] += 1
                    continue

                tree = fetch_tree(pool, owner, name, branch)
                if tree is None:
                    send_done(control_producer, repo, stage="tests", status="skipped")
                    consumer.acknowledge(msg)
                    stats["skipped"] += 1
                    continue

                found_test, found_ci = classify_tree(tree)

                enriched = {
                    "repo_id":    repo.get("id"),
                    "full_name":  repo.get("full_name"),
                    "language":   repo.get("language"),
                    "has_tests":  found_test,
                    "has_ci":     found_ci,
                }

                # Routing:
                #   test only       → test-topic                (Q3)
                #   test + ci       → test-topic + test-and-ci  (Q3 + Q4)
                #   ci only / none  → dropped (per project spec)
                if found_test:
                    test_producer.send(
                        json.dumps(enriched).encode("utf-8"),
                        properties={"repo_id": str(repo.get("id"))},
                    )
                    if found_ci:
                        test_ci_producer.send(
                            json.dumps(enriched).encode("utf-8"),
                            properties={"repo_id": str(repo.get("id"))},
                        )
                        stats["test_and_ci"] += 1
                    else:
                        stats["test_only"] += 1
                elif found_ci:
                    stats["ci_only"] += 1
                else:
                    stats["neither"] += 1

                send_done(control_producer, repo, stage="tests", status="ok")
                consumer.acknowledge(msg)

                stats["total"] += 1

                if stats["total"] % LOG_EVERY == 0:
                    print(f"  total={stats['total']}  test_only={stats['test_only']}  "
                          f"test_and_ci={stats['test_and_ci']}  ci_only={stats['ci_only']}  "
                          f"neither={stats['neither']}  skipped={stats['skipped']}", flush=True)

            except requests.exceptions.RequestException as e:
                print(f"  network error: {e}", flush=True)
                consumer.negative_acknowledge(msg)
            except Exception as e:
                print(f"  unexpected error: {e}", flush=True)
                consumer.negative_acknowledge(msg)

    except KeyboardInterrupt:
        print(f"\nFinal stats: {stats}", flush=True)
    finally:
        control_producer.close()
        test_producer.close()
        test_ci_producer.close()
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
