import json
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

RESULTS_FILE = os.environ.get("RESULTS_FILE", "results.json")
FIGURES_DIR  = os.environ.get("FIGURES_DIR", "figures")
TOP_N        = int(os.environ.get("TOP_N", "10"))

os.makedirs(FIGURES_DIR, exist_ok=True)


def bar_chart(labels, values, title, xlabel, color, filename):
    if not labels or not values:
        print(f"No data for {filename}, skipping.")
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(labels[::-1], values[::-1], color=color, edgecolor="none", height=0.6)

    for bar, val in zip(bars, values[::-1]):
        ax.text(
            bar.get_width() + max(values) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{val:,}", va="center", ha="left", fontsize=9, color="#555555"
        )

    ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    ax.set_xlabel(xlabel, fontsize=10)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(axis="y", labelsize=10)
    ax.tick_params(axis="x", labelsize=9)
    ax.set_xlim(0, max(values) * 1.18)

    fig.tight_layout()
    path = os.path.join(FIGURES_DIR, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def main():
    print(f"Reading {RESULTS_FILE}...")
    with open(RESULTS_FILE, "r") as f:
        data = json.load(f)

    # Q1
    q1 = data.get("q1_top_languages", [])[:TOP_N]
    bar_chart(
        [x[0] for x in q1], [x[1] for x in q1],
        f"Q1: Top {TOP_N} programming languages by project count",
        "Number of repositories", "#4C6EF5", "q1_languages.pdf"
    )

    # Q2
    q2 = data.get("q2_top_commits", [])[:TOP_N]
    bar_chart(
        [x[0].split("/")[-1] for x in q2], [x[1] for x in q2],
        f"Q2: Top {TOP_N} most frequently updated projects",
        "Total commit count", "#F76707", "q2_commits.pdf"
    )

    # Q3
    q3 = data.get("q3_tdd_languages", [])[:TOP_N]
    bar_chart(
        [x[0] for x in q3], [x[1] for x in q3],
        f"Q3: Top {TOP_N} languages following test-driven development",
        "Repositories with unit tests", "#2F9E44", "q3_tdd.pdf"
    )

    # Q4
    q4 = data.get("q4_devops_languages", [])[:TOP_N]
    bar_chart(
        [x[0] for x in q4], [x[1] for x in q4],
        f"Q4: Top {TOP_N} languages combining TDD and CI/CD",
        "Repositories with tests and CI", "#AE3EC9", "q4_devops.pdf"
    )

    print(f"\nDone! Figures saved to {FIGURES_DIR}/")


if __name__ == "__main__":
    main()