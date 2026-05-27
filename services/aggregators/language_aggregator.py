#!/usr/bin/env python3
"""
Q1 — top languages by project count. Subscribes to `repos.raw`, counts
repos per language, and participates in the producer's DONE protocol.
"""
import os
import json
import time
import collections
import pulsar

PULSAR_URL    = os.environ["PULSAR_URL"]
TOPIC         = os.environ.get("TOPIC", "repos.raw")
CONTROL_TOPIC = os.environ.get("CONTROL_TOPIC", "repos.raw.control")
SUBSCRIPTION  = os.environ.get("SUBSCRIPTION", "language-aggregator-sub")
TOP_N         = int(os.environ.get("TOP_N", "10"))
REPORT_EVERY  = int(os.environ.get("REPORT_EVERY", "100"))
RESULTS_FILE  = os.environ.get("RESULTS_FILE", "results_q1.json")


def render_top(counter, n):
    total = sum(counter.values())
    lines = [f"── Top {n} languages (out of {total} repos) ──"]
    for i, (lang, count) in enumerate(counter.most_common(n), 1):
        pct   = 100 * count / total if total else 0
        label = lang if lang else "(none)"
        lines.append(f"  {i:2d}. {label:<20s}  {count:>6d}  ({pct:.1f}%)")
    return "\n".join(lines)


def send_done(control_producer, repo, stage, status="ok", error=None):
    event = {
        "type":      "DONE",
        "run_id":    repo.get("run_id"),
        "job_id":    repo.get("job_id"),
        "repo_id":   str(repo.get("id")),
        "full_name": repo.get("full_name"),
        "stage":     stage,
        "status":    status,
        "error":     str(error) if error else None,
        "ts":        time.time(),
    }
    control_producer.send(
        json.dumps(event).encode("utf-8"),
        properties={
            "type":    "DONE",
            "stage":   stage,
            "status":  status,
            "run_id":  str(repo.get("run_id")),
            "job_id":  str(repo.get("job_id")),
            "repo_id": str(repo.get("id")),
        },
    )


def save_results(counter, n):
    data = {"q1_top_languages": counter.most_common(n)}
    with open(RESULTS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Results saved to {RESULTS_FILE}", flush=True)


def main():
    print(f"Connecting to Pulsar at {PULSAR_URL}", flush=True)
    client = pulsar.Client(PULSAR_URL)

    consumer = client.subscribe(
        TOPIC,
        subscription_name=SUBSCRIPTION,
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest,
        receiver_queue_size=1,
    )
    control_producer = client.create_producer(CONTROL_TOPIC)

    print(f"Subscribed to '{TOPIC}' as '{SUBSCRIPTION}'", flush=True)
    print(f"Sending DONE events to '{CONTROL_TOPIC}'", flush=True)
    print(f"Reporting top {TOP_N} every {REPORT_EVERY} messages", flush=True)

    counter = collections.Counter()
    seen    = 0

    try:
        while True:
            msg = consumer.receive()
            try:
                repo = json.loads(msg.data().decode("utf-8"))
                counter[repo.get("language")] += 1
                send_done(control_producer, repo, stage="language", status="ok")
                consumer.acknowledge(msg)
                seen += 1
                if seen % REPORT_EVERY == 0:
                    print("\n" + render_top(counter, TOP_N), flush=True)
                    save_results(counter, TOP_N)
            except Exception as e:
                print(f"  error: {e}", flush=True)
                consumer.negative_acknowledge(msg)

    except KeyboardInterrupt:
        print("\nFinal", flush=True)
        print(render_top(counter, TOP_N), flush=True)
        save_results(counter, TOP_N)

    finally:
        control_producer.close()
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
