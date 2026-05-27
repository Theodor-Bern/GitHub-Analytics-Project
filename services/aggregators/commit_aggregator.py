#!/usr/bin/env python3
"""
Q2 — most frequently updated projects. Subscribes to `commit-topic` and
keeps the top-N repos by commit count.
"""
import os
import json
import pulsar

PULSAR_URL   = os.environ["PULSAR_URL"]
TOPIC        = os.environ.get("TOPIC", "commit-topic")
SUBSCRIPTION = os.environ.get("SUBSCRIPTION", "commit-aggregator-sub")
TOP_N        = int(os.environ.get("TOP_N", "10"))
REPORT_EVERY = int(os.environ.get("REPORT_EVERY", "50"))
RESULTS_FILE = os.environ.get("RESULTS_FILE", "results_q2.json")


def render_top(repos_dict, n):
    sorted_repos = sorted(repos_dict.values(), key=lambda r: r["commit_count"], reverse=True)
    lines = [f"── Top {n} repos by commit count (out of {len(repos_dict)} seen) ──"]
    for i, repo in enumerate(sorted_repos[:n], 1):
        lang = repo.get("language") or "(none)"
        lines.append(
            f"  {i:2d}. {repo['full_name']:<50s} {repo['commit_count']:>8d} commits  [{lang}]"
        )
    return "\n".join(lines)


def save_results(repos_dict, n):
    sorted_repos = sorted(repos_dict.values(), key=lambda r: r["commit_count"], reverse=True)
    top = [[r["full_name"], r["commit_count"]] for r in sorted_repos[:n]]
    data = {"q2_top_commits": top}
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

    repos = {}
    seen  = 0

    try:
        while True:
            msg = consumer.receive()
            try:
                repo = json.loads(msg.data().decode("utf-8"))
                repos[repo["repo_id"]] = repo
                consumer.acknowledge(msg)
                seen += 1
                if seen % REPORT_EVERY == 0:
                    print("\n" + render_top(repos, TOP_N), flush=True)
                    save_results(repos, TOP_N)
            except Exception as e:
                print(f"  error: {e}", flush=True)
                consumer.negative_acknowledge(msg)

    except KeyboardInterrupt:
        print("\nFinal", flush=True)
        print(render_top(repos, TOP_N), flush=True)
        save_results(repos, TOP_N)

    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
