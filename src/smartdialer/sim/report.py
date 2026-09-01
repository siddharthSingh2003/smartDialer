import csv
import os
from collections import Counter

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load_csv(path: str) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _f(row, key):
    return float(row[key])


def plot_utilization(rows: list[dict], out_path: str, title: str) -> None:
    t = [_f(r, "sim_time") for r in rows]
    u = [_f(r, "utilization") * 100 for r in rows]
    plt.figure(figsize=(9, 4))
    plt.plot(t, u, color="#2563eb", linewidth=1.5)
    plt.title(f"Agent utilization — {title}")
    plt.xlabel("sim time (s)")
    plt.ylabel("utilization (%)")
    plt.ylim(0, 105)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=130)
    plt.close()


def plot_utilization_comparison(pred_rows: list[dict], prog_rows: list[dict], out_path: str) -> None:
    plt.figure(figsize=(9, 4))
    plt.plot([_f(r, "sim_time") for r in pred_rows], [_f(r, "utilization") * 100 for r in pred_rows],
              color="#2563eb", linewidth=1.5, label="predictive")
    plt.plot([_f(r, "sim_time") for r in prog_rows], [_f(r, "utilization") * 100 for r in prog_rows],
              color="#9ca3af", linewidth=1.5, linestyle="--", label="progressive (baseline)")
    plt.title("Agent utilization — predictive vs progressive (same seed)")
    plt.xlabel("sim time (s)")
    plt.ylabel("utilization (%)")
    plt.ylim(0, 105)
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=130)
    plt.close()


def plot_requested_vs_approved(rows: list[dict], out_path: str, title: str) -> None:
    t = [_f(r, "sim_time") for r in rows]
    req = [_f(r, "requested") for r in rows]
    app = [_f(r, "approved") for r in rows]
    plt.figure(figsize=(9, 4))
    plt.plot(t, req, color="#f97316", linewidth=1.2, label="requested")
    plt.plot(t, app, color="#16a34a", linewidth=1.2, label="approved")
    plt.fill_between(t, app, req, where=[r >= a for r, a in zip(req, app)],
                      color="#f97316", alpha=0.15, label="cut by safety controller")
    plt.title(f"Requested vs approved — {title}")
    plt.xlabel("sim time (s)")
    plt.ylabel("calls / tick")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=130)
    plt.close()


def plot_reason_codes(rows: list[dict], out_path: str, title: str, buckets: int = 40) -> None:
    if not rows:
        return
    t_max = _f(rows[-1], "sim_time") or 1.0
    width = max(t_max / buckets, 1e-6)
    codes = sorted({r["reason_code"] for r in rows})
    series = {c: [0] * buckets for c in codes}
    for r in rows:
        b = min(buckets - 1, int(_f(r, "sim_time") / width))
        series[r["reason_code"]][b] += 1
    xs = [i * width for i in range(buckets)]
    plt.figure(figsize=(9, 4))
    plt.stackplot(xs, *[series[c] for c in codes], labels=codes)
    plt.title(f"Binding safety rule over time — {title}")
    plt.xlabel("sim time (s)")
    plt.ylabel("ticks in bucket")
    plt.legend(loc="upper right", fontsize=7, ncol=2)
    plt.tight_layout()
    plt.savefig(out_path, dpi=130)
    plt.close()


def plot_abandonment(rows: list[dict], out_path: str, title: str, budget: float = 0.03) -> None:
    t = [_f(r, "sim_time") for r in rows]
    ab = [_f(r, "abandon_rate_5m") * 100 for r in rows]
    plt.figure(figsize=(9, 4))
    plt.plot(t, ab, color="#dc2626", linewidth=1.3, label="abandon rate")
    plt.axhline(budget * 100, color="#111827", linestyle="--", linewidth=1, label=f"{budget*100:.0f}% budget")
    plt.title(f"Abandonment rate — {title}")
    plt.xlabel("sim time (s)")
    plt.ylabel("abandon rate (%)")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=130)
    plt.close()


def summarize(rows: list[dict]) -> dict:
    if not rows:
        return {}
    last = rows[-1]
    util = [_f(r, "utilization") for r in rows]
    counts = Counter(r["reason_code"] for r in rows)
    dominant = counts.most_common(1)[0][0] if counts else "n/a"
    connected = int(_f(last, "calls_connected_cum"))
    abandoned = int(_f(last, "calls_abandoned_cum"))
    total_resolved = connected + abandoned
    return {
        "mean_utilization": sum(util) / len(util),
        "final_utilization": util[-1],
        "calls_connected": connected,
        "calls_abandoned": abandoned,
        "abandon_pct": (abandoned / total_resolved * 100) if total_resolved else 0.0,
        "dominant_reason_code": dominant,
        "safety_interventions": sum(1 for r in rows if r["reason_code"] != "OK"),
        "ticks": len(rows),
    }


def generate_report(results_dir: str) -> str:
    """Reads every <scenario>.csv already written to `results_dir`, emits the
    four charts per scenario plus the B-vs-G comparison, and returns a
    markdown summary table."""
    files = sorted(f for f in os.listdir(results_dir) if f.endswith(".csv"))
    summary_lines = ["| Scenario | Mean utilization | Calls connected | Abandon % | Dominant reason code |",
                      "|---|---|---|---|---|"]
    all_rows: dict[str, list[dict]] = {}
    for fname in files:
        name = fname[:-4]
        rows = load_csv(os.path.join(results_dir, fname))
        all_rows[name] = rows
        if not rows:
            continue
        plot_utilization(rows, os.path.join(results_dir, f"{name}_utilization.png"), name)
        plot_requested_vs_approved(rows, os.path.join(results_dir, f"{name}_requested_vs_approved.png"), name)
        plot_reason_codes(rows, os.path.join(results_dir, f"{name}_reason_codes.png"), name)
        plot_abandonment(rows, os.path.join(results_dir, f"{name}_abandonment.png"), name)
        s = summarize(rows)
        summary_lines.append(
            f"| {name} | {s['mean_utilization']*100:.1f}% | {s['calls_connected']} | "
            f"{s['abandon_pct']:.2f}% | {s['dominant_reason_code']} |")

    if all_rows.get("B") and all_rows.get("G_progressive_baseline"):
        plot_utilization_comparison(all_rows["B"], all_rows["G_progressive_baseline"],
                                     os.path.join(results_dir, "B_vs_G_utilization.png"))
        sb, sg = summarize(all_rows["B"]), summarize(all_rows["G_progressive_baseline"])
        summary_lines += ["", "### Predictive (B) vs progressive baseline (G), same seed", "",
                           "| | progressive | predictive | delta |", "|---|---|---|---|",
                           f"| utilization | {sg['mean_utilization']*100:.1f}% | "
                           f"{sb['mean_utilization']*100:.1f}% | "
                           f"{(sb['mean_utilization']-sg['mean_utilization'])*100:+.1f}pp |",
                           f"| calls connected | {sg['calls_connected']} | {sb['calls_connected']} | "
                           f"{sb['calls_connected']-sg['calls_connected']:+d} |",
                           f"| abandonment | {sg['abandon_pct']:.2f}% | {sb['abandon_pct']:.2f}% | "
                           f"within {sb['abandon_pct']:.2f}% budget (3%) |",
                           f"| safety interventions | {sg['safety_interventions']} | "
                           f"{sb['safety_interventions']} | of {sb['ticks']} ticks |"]

    summary_md = "\n".join(summary_lines) + "\n"
    with open(os.path.join(results_dir, "summary.md"), "w") as f:
        f.write(summary_md)
    return summary_md
