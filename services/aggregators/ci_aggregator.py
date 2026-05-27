#!/usr/bin/env python3
"""
Q4 — top languages following TDD + DevOps. Subscribes to `test-and-ci-topic`
(repos with both tests and CI) and counts the language distribution.
"""
import os
import json
import collections
import pulsar

PULSAR_URL   = os.environ["PULSAR_URL"]
TOPIC        = os.environ.get("TOPIC", "test-and-ci-topic")
SUBSCRIPTION = os.environ.get("SUBSCRIPTION", "ci-aggregator-sub")
TOP_N        = int(os.environ.get("TOP_N", "10"))
REPORT_EVERY = int(os.environ.get("REPORT_EVERY", "25"))
RESULTS_FILE = os.environ.get("RESULTS_FILE", "results_q4.json")


def render_top(counter, n):
    total = sum(counter.values())
    lines = [f"── Top {n} languages with tests + CI (out of {total} repos) ──"]
    for i, (lang, count) in enumerate(counter.most_common(n), 1):
        pct   = 100 * count / total if total else 0
        label = lang if lang else "(none)"
        lines.append(f"  {i:2d}. {label:<20s}  {count:>6d}  ({pct:.1f}%)")
    return "\n".join(lines)


def save_results(counter, n):
    data = {"q4_devops_languages": counter.most_common(n)}
    with open(RESULTS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Results saved to {RESULTS_FILE}", flush=True)


def main():
    print(f"Connecting to Pulsar at {PULSAR_URL}", flush=True)
    client   = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        TOPIC,
        subscription_name=SUBSCRIPTION,
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest,
    )
    print(f"Subscribed to '{TOPIC}' as '{SUBSCRIPTION}'", flush=True)

    seen_ids = set()
    counter  = collections.Counter()

    try:
        while True:
            msg = consumer.receive()
            try:
                repo = json.loads(msg.data().decode("utf-8"))
                rid  = repo.get("repo_id")
                if rid not in seen_ids:
                    seen_ids.add(rid)
                    counter[repo.get("language")] += 1
                consumer.acknowledge(msg)
                if len(seen_ids) % REPORT_EVERY == 0:
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
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
