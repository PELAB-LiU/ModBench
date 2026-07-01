#!/usr/bin/env python3
"""Unified report script for the SAM2026 paper.

Runs SQL queries against the pipeline databases (local sqlite3 by default,
or remote via SSH when ``USE_REMOTE=1``), computes statistics, updates
``variables.tex``, generates figures in ``figs/``, and prints the
failure-analysis breakdown.

Usage:
    python3 reports.py

Environment variables (override via shell or ``.env``):
    USE_REMOTE           Set to 1 to read databases over SSH instead of locally
    REMOTE_HOST          SSH host alias (required when USE_REMOTE=1)
    REMOTE_BASE          Remote base path containing ``dataset/`` (required when USE_REMOTE=1)
    LOCAL_SOURCE_DIR     Local path to Modelica source repos for git date resolution
"""
from __future__ import annotations

import csv
import io
import os
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median

PAPER_DIR = Path(__file__).resolve().parent
FIG_DIR = PAPER_DIR / "figs"
VARIABLES_TEX = PAPER_DIR / "variables.tex"

# Load .env (if present) so users don't have to export shell variables.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(PAPER_DIR / ".env")
except ImportError:
    pass

REMOTE_HOST = os.environ.get("REMOTE_HOST", "").strip()
REMOTE_BASE = os.environ.get("REMOTE_BASE", "").strip()
REMOTE_PIPELINE_DB = f"{REMOTE_BASE}/dataset/pipeline.db" if REMOTE_BASE else ""
REMOTE_STEP2_DB = f"{REMOTE_BASE}/dataset/step2_classes.db" if REMOTE_BASE else ""

SAM2026_ROOT = Path(__file__).resolve().parent
PIPELINE_DB = str(SAM2026_ROOT / "dataset" / "pipeline.db")
STEP2_DB = str(SAM2026_ROOT / "dataset" / "step2_classes.db")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Default to local sqlite unless explicitly enabled for SSH mode.
USE_REMOTE = _env_bool("USE_REMOTE", default=False)
if USE_REMOTE and (not REMOTE_HOST or not REMOTE_BASE):
    raise RuntimeError(
        "USE_REMOTE=1 requires REMOTE_HOST and REMOTE_BASE to be set "
        "(via environment or .env). See .env.example."
    )

# Temporary shortcut: keep enabled for now to regenerate only the main
# step2 timeline figure without running full report stages.
ONLY_STEP2_TIMELINE_FIGURE = True

ACTIVE_PIPELINE_DB = REMOTE_PIPELINE_DB if USE_REMOTE else PIPELINE_DB
ACTIVE_STEP2_DB = REMOTE_STEP2_DB if USE_REMOTE else STEP2_DB

# Local source repositories for git date resolution
LOCAL_SOURCE_DIR = Path(
    os.environ.get("LOCAL_SOURCE_DIR", str(PAPER_DIR / "source"))
).resolve()

SOURCE_NAME = "MSL"


def _is_figure_only_mode(argv: list[str]) -> bool:
    return "--figure-only" in argv


# ---------------------------------------------------------------------------
# SQL execution (local sqlite3 or remote via SSH)
# ---------------------------------------------------------------------------


def _remote_sql(db_path: str, sql: str, timeout: int = 300) -> list[dict[str, str]]:
    """Execute a SQL query and return rows as dicts.

    Runs locally via the ``sqlite3`` CLI unless ``USE_REMOTE`` is set, in
    which case the query is executed on ``REMOTE_HOST`` via SSH.
    """
    if USE_REMOTE:
        cmd_args = ["ssh", REMOTE_HOST, f'sqlite3 -header -csv "{db_path}" "{sql}"']
    else:
        cmd_args = ["sqlite3", "-header", "-csv", db_path, sql]
    result = subprocess.run(
        cmd_args, capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise RuntimeError(f"SQL failed on {db_path}: {stderr}\nSQL: {sql}")
    output = result.stdout.strip()
    if not output:
        return []
    reader = csv.DictReader(io.StringIO(output))
    return [dict(row) for row in reader]


def _remote_sql_value(db_path: str, sql: str) -> str:
    """Execute a single-value query remotely and return the result."""
    rows = _remote_sql(db_path, sql)
    if rows:
        return list(rows[0].values())[0]
    return "0"


# ---------------------------------------------------------------------------
# Git date resolution (local)
# ---------------------------------------------------------------------------


def _get_commit_dates(commit_hashes: list[str]) -> dict[str, str]:
    """Resolve commit hashes to YYYY-MM-DD dates using the local MSL repo."""
    repo_path = LOCAL_SOURCE_DIR / SOURCE_NAME
    if not repo_path.exists():
        print(f"[WARN]  Local repo not found at {repo_path}, skipping date resolution")
        return {}
    try:
        import git as gitpython
    except ImportError:
        print("[WARN]  gitpython not installed, skipping date resolution")
        return {}
    try:
        repo = gitpython.Repo(repo_path)
    except Exception as e:
        print(f"[WARN]  Cannot open repo: {e}")
        return {}

    dates: dict[str, str] = {}
    hash_set = set(commit_hashes)
    for commit in repo.iter_commits("--all", max_count=50_000):
        h = commit.hexsha
        if h in hash_set:
            dates[h] = datetime.fromtimestamp(
                commit.committed_date, tz=timezone.utc
            ).strftime("%Y-%m-%d")
            hash_set.discard(h)
            if not hash_set:
                break
    return dates


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pct(n: int, d: int) -> float:
    return (100.0 * n / d) if d else 0.0


def _fmt_int(n: int) -> str:
    """Format integer with {,} thousand separators for LaTeX."""
    s = f"{n:,}"
    return s.replace(",", "{,}")


def _int(s: str) -> int:
    """Parse a string to int, tolerating commas and whitespace."""
    return int(s.replace(",", "").strip() or "0")


# ---------------------------------------------------------------------------
# Step 1: Filter commits
# ---------------------------------------------------------------------------


def analyze_step1() -> dict[str, str]:
    print("  Querying step1_commits...")
    total_unique = _int(_remote_sql_value(
        ACTIVE_PIPELINE_DB,
        f"SELECT COUNT(*) FROM step1_commits WHERE source_name='{SOURCE_NAME}'"
    ))
    included = _int(_remote_sql_value(
        ACTIVE_PIPELINE_DB,
        f"SELECT COUNT(*) FROM step1_commits WHERE source_name='{SOURCE_NAME}' AND excluded=0"
    ))
    exclusion_rows = _remote_sql(
        ACTIVE_PIPELINE_DB,
        f"SELECT exclusion_reason, COUNT(*) AS cnt FROM step1_commits "
        f"WHERE source_name='{SOURCE_NAME}' AND excluded=1 GROUP BY exclusion_reason"
    )

    excluded_branches = excluded_pre_v3 = excluded_bot = excluded_no_mo = 0
    for r in exclusion_rows:
        reason = r["exclusion_reason"].lower()
        cnt = int(r["cnt"])
        if "excluded branch" in reason:
            excluded_branches += cnt
        elif "before tag" in reason:
            excluded_pre_v3 += cnt
        elif "no .mo files" in reason:
            excluded_no_mo += cnt
        elif any(k in reason for k in ("bot", "commit message matches", "author name contains", "author email contains")):
            excluded_bot += cnt

    retained_pct = _pct(included, total_unique)

    return {
        "MbFilterInputCommits": _fmt_int(total_unique),
        "MbFilterExclBranches": _fmt_int(excluded_branches),
        "MbFilterAfterBranches": _fmt_int(total_unique - excluded_branches),
        "MbFilterExclPreVThree": _fmt_int(excluded_pre_v3),
        "MbFilterAfterPreVThree": _fmt_int(total_unique - excluded_branches - excluded_pre_v3),
        "MbFilterExclBot": _fmt_int(excluded_bot),
        "MbFilterAfterBot": _fmt_int(total_unique - excluded_branches - excluded_pre_v3 - excluded_bot),
        "MbFilterExclNoMo": _fmt_int(excluded_no_mo),
        "MbFilterRetained": _fmt_int(included),
        "MbFilterRetainedPct": f"{retained_pct:.1f}",
    }


# ---------------------------------------------------------------------------
# Step 2: Class enumeration
# ---------------------------------------------------------------------------


def analyze_step2() -> tuple[dict[str, str], list[dict[str, str]]]:
    print("  Querying step2_classes (summary)...")
    summary = _remote_sql(
        ACTIVE_STEP2_DB,
        f"SELECT COUNT(*) AS total_rows, COUNT(DISTINCT commit_hash) AS processed_commits, "
        f"COUNT(DISTINCT class_name) AS unique_classes, "
        f"SUM(CASE WHEN is_experiment=1 THEN 1 ELSE 0 END) AS experiment_rows, "
        f"SUM(CASE WHEN is_experiment=0 THEN 1 ELSE 0 END) AS non_experiment_rows "
        f"FROM step2_classes WHERE source_name='{SOURCE_NAME}'"
    )[0]

    total_rows = _int(summary["total_rows"])
    unique_classes = _int(summary["unique_classes"])
    experiment_rows = _int(summary["experiment_rows"])
    non_experiment_rows = _int(summary["non_experiment_rows"])

    print("  Querying per-commit breakdown (this may take a while)...")
    per_commit_rows = _remote_sql(
        ACTIVE_STEP2_DB,
        f"SELECT commit_hash, COUNT(*) AS total_classes, "
        f"SUM(CASE WHEN is_experiment=1 THEN 1 ELSE 0 END) AS experiment_classes "
        f"FROM step2_classes WHERE source_name='{SOURCE_NAME}' "
        f"GROUP BY commit_hash",
        timeout=600,
    )

    totals = [_int(r["total_classes"]) for r in per_commit_rows]
    experiments = [_int(r["experiment_classes"]) for r in per_commit_rows]
    ratios = [e / t if t > 0 else 0.0 for t, e in zip(totals, experiments)]

    values = {
        "MbListDistinctClassNames": _fmt_int(unique_classes),
        "MbListAllClassVersions": _fmt_int(total_rows),
        "MbListNonExperimentVersions": _fmt_int(non_experiment_rows),
        "MbListNonExperimentPct": f"{_pct(non_experiment_rows, total_rows):.1f}",
        "MbListExperimentVersions": _fmt_int(experiment_rows),
        "MbListExperimentPct": f"{_pct(experiment_rows, total_rows):.1f}",
        "MbListMeanExperimentPerCommit": str(round(mean(experiments))) if experiments else "0",
        "MbListMaxExperimentPerCommit": str(max(experiments)) if experiments else "0",
        # Growth stats
        "MbGrowthAllClassMean": _fmt_int(round(mean(totals))) if totals else "0",
        "MbGrowthAllClassMedian": _fmt_int(round(median(totals))) if totals else "0",
        "MbGrowthAllClassMax": _fmt_int(max(totals)) if totals else "0",
        "MbGrowthExperimentMedian": str(round(median(experiments))) if experiments else "0",
        "MbGrowthExperimentRatioMean": f"{mean(ratios) * 100:.1f}" if ratios else "0",
    }
    return values, per_commit_rows


# ---------------------------------------------------------------------------
# Step 3: Canonicalization
# ---------------------------------------------------------------------------


def _count_commit_level_load_failures() -> int:
    """Count step2 experiment-class rows belonging to commits where step3
    produced zero rows (commit-level package-load failures).

    These failures are recorded in `step3_failures` with an empty
    `class_name` and never make it into `step3_classes`, so the standard
    per-class failure query in `analyze_step3` misses them. We attribute the
    full per-commit experiment count of each such commit to the failure
    bucket so the totals reconcile with `\\MbListExperimentVersions`.
    """
    sql = (
        f"ATTACH '{ACTIVE_PIPELINE_DB}' AS pl; "
        f"SELECT COALESCE(SUM(cnt), 0) FROM ("
        f"  SELECT commit_hash, COUNT(*) AS cnt "
        f"  FROM step2_classes "
        f"  WHERE source_name='{SOURCE_NAME}' AND is_experiment=1 "
        f"    AND commit_hash NOT IN ("
        f"      SELECT DISTINCT commit_hash FROM pl.step3_classes "
        f"      WHERE source_name='{SOURCE_NAME}'"
        f"    ) "
        f"  GROUP BY commit_hash"
        f");"
    )
    return _int(_remote_sql_value(ACTIVE_STEP2_DB, sql))


def analyze_step3(extra_load_failures: int = 0) -> dict[str, str]:
    print("  Querying step3_classes...")
    step3 = _remote_sql(
        ACTIVE_PIPELINE_DB,
        f"SELECT SUM(CASE WHEN canonical_produced=1 THEN 1 ELSE 0 END) AS saved_rows, "
        f"SUM(CASE WHEN canonical_produced=0 THEN 1 ELSE 0 END) AS failure_rows "
        f"FROM step3_classes WHERE source_name='{SOURCE_NAME}'"
    )[0]

    saved_rows = _int(step3["saved_rows"])
    # Per-class failures from step3_classes PLUS commit-level package-load
    # failures attributed from step3_failures (one commit -> N
    # experiment classes never reached canonicalization).
    failure_rows = _int(step3["failure_rows"]) + extra_load_failures
    processed = saved_rows + failure_rows

    commits_with_exp = _int(_remote_sql_value(
        ACTIVE_STEP2_DB,
        f"SELECT COUNT(DISTINCT commit_hash) FROM step2_classes "
        f"WHERE source_name='{SOURCE_NAME}' AND is_experiment=1"
    ))

    total_filtered = _int(_remote_sql_value(
        ACTIVE_PIPELINE_DB,
        f"SELECT COUNT(*) FROM step1_commits WHERE source_name='{SOURCE_NAME}' AND excluded=0"
    ))

    return {
        "MbCanonCommitsWithExperiment": _fmt_int(commits_with_exp),
        "MbCanonCommitsWithExperimentPct": f"{_pct(commits_with_exp, total_filtered):.1f}",
        "MbCanonProducedVersions": _fmt_int(saved_rows),
        "MbCanonProducedPct": f"{_pct(saved_rows, processed):.1f}",
        "MbCanonFailures": _fmt_int(failure_rows),
        "MbCanonFailuresPct": f"{_pct(failure_rows, processed):.1f}",
    }


# ---------------------------------------------------------------------------
# Failure Analysis
# ---------------------------------------------------------------------------

FAILURE_PATTERNS: list[tuple[str, str]] = [
    (r"(?i)loadFile|loadModel|could not (load|open)|package.*not found|failed to load",
     "Package load failure"),
    (r"(?i)extends.*not found|base.?class.*not|could not find.*base|unknown base",
     "Missing base class"),
    (r"(?i)class.*not found|not found in scope|could not find class|unknown class|lookup.*failed",
     "Missing referenced class"),
    (r"(?i)replaceable|redeclare|redeclaration|not replaceable",
     "Replaceable/redeclare conflict"),
    (r"(?i)restriction|operator.?record|illegal.*operator",
     "Operator-record restriction"),
    (r"(?i)type\s+error|type.*mismatch|incompatible.*type",
     "Type mismatch"),
    (r"(?i)savetotalmodel|save.*total.*model|timeout|timed out",
     "Canonicalization timeout"),
    (r"(?i)syntax|parse\s*error|unexpected token",
     "Syntax error"),
]


def analyze_failures(extra_load_failures: int = 0) -> tuple[dict[str, int], int]:
    """Categorize canonicalization failures from step3_classes error messages.

    ``extra_load_failures`` is the number of commit-level package-load
    failures attributed from ``step3_failures`` (one such
    failure invalidates an entire commit). They are folded into the
    ``Package load failure`` category so the per-category counts sum to
    the failure total reported in ``analyze_step3``.
    """
    print("  Querying failure error messages...")
    rows = _remote_sql(
        ACTIVE_PIPELINE_DB,
        f"SELECT error_message FROM step3_classes "
        f"WHERE source_name='{SOURCE_NAME}' AND canonical_produced=0 AND error_message!=''",
        timeout=600,
    )

    total_failures = len(rows)
    categories: Counter = Counter()

    for r in rows:
        msg = r.get("error_message", "").strip()
        if not msg:
            categories["Empty error / unknown"] += 1
            continue
        matched = False
        for pattern, label in FAILURE_PATTERNS:
            if re.search(pattern, msg):
                categories[label] += 1
                matched = True
                break
        if not matched:
            categories["Other"] += 1

    if extra_load_failures:
        categories["Package load failure"] += extra_load_failures
        total_failures += extra_load_failures

    return dict(categories.most_common()), total_failures


# ---------------------------------------------------------------------------
# On-disk artifact sizes (from remote server)
# ---------------------------------------------------------------------------


def get_remote_artifact_sizes() -> dict[str, str]:
    """Query artifact sizes (local ``du`` or remote via SSH)."""
    values = {}
    if USE_REMOTE:
        cmds = {
            "MbStorePipelineDbSize": f"du -sh {REMOTE_BASE}/dataset/pipeline.db | cut -f1",
            "MbStoreClassEnumDbSize": f"du -sh {REMOTE_BASE}/dataset/step2_classes.db | cut -f1",
            "MbStoreCanonicalDirSize": f"du -sh {REMOTE_BASE}/canonical_models/MSL/ | cut -f1",
        }
    else:
        local_dataset = SAM2026_ROOT / "dataset"
        cmds = {
            "MbStorePipelineDbSize": f"du -sh {local_dataset / 'pipeline.db'} | cut -f1",
            "MbStoreClassEnumDbSize": f"du -sh {local_dataset / 'step2_classes.db'} | cut -f1",
            "MbStoreCanonicalDirSize": f"du -sh {local_dataset / 'canonical_models' / 'MSL'} | cut -f1",
        }
    for var, cmd in cmds.items():
        try:
            if USE_REMOTE:
                result = subprocess.run(
                    ["ssh", REMOTE_HOST, cmd],
                    capture_output=True, timeout=60, text=True,
                )
            else:
                result = subprocess.run(
                    cmd, shell=True, capture_output=True, timeout=60, text=True,
                )
            if result.returncode == 0:
                size_str = result.stdout.strip()
                m = re.match(r"([\d.]+)([KMGT]?)", size_str)
                if m:
                    num, unit = m.groups()
                    unit_map = {"K": "KB", "M": "MB", "G": "GB", "T": "TB", "": "B"}
                    values[var] = num + r"\," + unit_map.get(unit, unit + "B")
        except Exception as e:
            print(f"[WARN]  Could not get size for {var}: {e}")
    return values


# ---------------------------------------------------------------------------
# Figure generation
# ---------------------------------------------------------------------------


def plot_step2_timeline(per_commit_rows: list[dict[str, str]], commit_hashes: list[str]) -> None:
    """Generate the two-panel classes timeline figure for the paper."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("[WARN]  matplotlib/numpy not installed, skipping figures.")
        return

    dates = _get_commit_dates(commit_hashes)
    if len(dates) < 10:
        print(f"[WARN]  Only {len(dates)} commit dates resolved, skipping timeline figure.")
        return

    # Build time series
    time_series = []
    for r in per_commit_rows:
        h = r["commit_hash"]
        d = dates.get(h)
        if d:
            time_series.append((d, _int(r["total_classes"]), _int(r["experiment_classes"])))
    time_series.sort(key=lambda x: x[0])

    if len(time_series) < 10:
        print("[WARN]  Insufficient time series data for figure.")
        return

    plot_dates = [datetime.strptime(d, "%Y-%m-%d") for d, _, _ in time_series]
    totals = [t for _, t, _ in time_series]
    experiments = [e for _, _, e in time_series]

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(7, 4.5), dpi=160, sharex=True,
        gridspec_kw={"hspace": 0.15}
    )

    window = min(50, len(totals) // 5) if len(totals) > 10 else 1

    # Top panel: all classes
    ax_top.scatter(plot_dates, totals, s=2, alpha=0.25, color="#1f77b4")
    if window > 1:
        kernel = np.ones(window) / window
        smooth = np.convolve(totals, kernel, mode="valid")
        offset = window // 2
        ax_top.plot(
            plot_dates[offset: offset + len(smooth)],
            smooth, color="#1f77b4", linewidth=1.5,
        )
    avg_total = mean(totals)
    ax_top.axhline(y=avg_total, color="#1f77b4", linestyle="--", linewidth=0.8, alpha=0.6)
    ax_top.set_ylabel("All classes", fontsize=10)
    ax_top.grid(True, alpha=0.3)
    ax_top.set_title("MSL class population per processed commit", fontsize=10)

    # Bottom panel: experiment classes
    ax_bot.scatter(plot_dates, experiments, s=2, alpha=0.25, color="#ff7f0e")
    if window > 1:
        smooth_exp = np.convolve(experiments, kernel, mode="valid")
        ax_bot.plot(
            plot_dates[offset: offset + len(smooth_exp)],
            smooth_exp, color="#ff7f0e", linewidth=1.5,
        )
    avg_exp = mean(experiments)
    ax_bot.axhline(y=avg_exp, color="#ff7f0e", linestyle="--", linewidth=0.8, alpha=0.6)
    ax_bot.set_ylabel("Simulation-eligible classes", fontsize=10)
    ax_bot.set_xlabel("Commit date")
    ax_bot.grid(True, alpha=0.3)

    # Keep both y-labels in the same vertical column.
    ax_top.yaxis.set_label_coords(-0.09, 0.5)
    ax_bot.yaxis.set_label_coords(-0.09, 0.5)

    fig.autofmt_xdate()
    fig.tight_layout()
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    out_path = FIG_DIR / "step2_classes_timeline.png"
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out_path}")


def plot_step2_timeline_v2(per_commit_rows: list[dict[str, str]], commit_hashes: list[str]) -> None:
    """Generate a single-panel timeline with two y-axes (all vs. experiment classes)."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("[WARN]  matplotlib/numpy not installed, skipping v2 figure.")
        return

    dates = _get_commit_dates(commit_hashes)
    if len(dates) < 10:
        print(f"[WARN]  Only {len(dates)} commit dates resolved, skipping v2 figure.")
        return

    time_series = []
    for r in per_commit_rows:
        h = r["commit_hash"]
        d = dates.get(h)
        if d:
            time_series.append((d, _int(r["total_classes"]), _int(r["experiment_classes"])))
    time_series.sort(key=lambda x: x[0])

    if len(time_series) < 10:
        print("[WARN]  Insufficient time series data for v2 figure.")
        return

    plot_dates = [datetime.strptime(d, "%Y-%m-%d") for d, _, _ in time_series]
    totals = [t for _, t, _ in time_series]
    experiments = [e for _, _, e in time_series]

    fig, ax_left = plt.subplots(figsize=(7, 3.2), dpi=160)
    ax_right = ax_left.twinx()

    window = min(50, len(totals) // 5) if len(totals) > 10 else 1
    color_all = "#1f77b4"
    color_exp = "#ff7f0e"

    # All classes (left axis)
    ax_left.scatter(plot_dates, totals, s=2, alpha=0.20, color=color_all)
    if window > 1:
        kernel = np.ones(window) / window
        smooth = np.convolve(totals, kernel, mode="valid")
        offset = window // 2
        line_all, = ax_left.plot(
            plot_dates[offset: offset + len(smooth)],
            smooth, color=color_all, linewidth=1.6, label="All classes",
        )
    else:
        line_all, = ax_left.plot(plot_dates, totals, color=color_all, linewidth=1.6, label="All classes")
    ax_left.set_ylabel("All classes", color=color_all)
    ax_left.tick_params(axis="y", labelcolor=color_all)
    ax_left.grid(False)

    # Experiment classes (right axis)
    ax_right.scatter(plot_dates, experiments, s=2, alpha=0.20, color=color_exp)
    if window > 1:
        smooth_exp = np.convolve(experiments, kernel, mode="valid")
        line_exp, = ax_right.plot(
            plot_dates[offset: offset + len(smooth_exp)],
            smooth_exp, color=color_exp, linewidth=1.6, label="Simulation-eligible classes",
        )
    else:
        line_exp, = ax_right.plot(plot_dates, experiments, color=color_exp, linewidth=1.6, label="Simulation-eligible classes")
    ax_right.set_ylabel("Simulation-eligible classes", color=color_exp)
    ax_right.tick_params(axis="y", labelcolor=color_exp)

    ax_left.set_xlabel("Commit date")
    ax_left.legend(
        [line_all, line_exp], ["All classes", "Simulation-eligible classes"],
        loc="upper left", frameon=True, fontsize=9,
    )

    fig.autofmt_xdate()
    fig.tight_layout()
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    out_path = FIG_DIR / "step2_classes_timeline_v2.png"
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out_path}")


# ---------------------------------------------------------------------------
# variables.tex update
# ---------------------------------------------------------------------------

NEWCOMMAND_PREFIX_RE = re.compile(r"^(\s*\\newcommand\{\\)([A-Za-z]+)\}\{")


def _parse_newcommand(line: str):
    """Parse a \\newcommand line, returning (prefix, macro_name, value, suffix) or None.

    Uses brace-counting to correctly handle nested braces in the value.
    """
    m = NEWCOMMAND_PREFIX_RE.match(line)
    if not m:
        return None
    prefix = m.group(1)      # e.g. "\\newcommand{\\"
    macro_name = m.group(2)  # e.g. "MbFilterInputCommits"
    # Start scanning after the opening '{' of the value
    start = m.end()  # position right after "}{" 
    depth = 1
    i = start
    while i < len(line) and depth > 0:
        if line[i] == '{':
            depth += 1
        elif line[i] == '}':
            depth -= 1
        i += 1
    if depth != 0:
        return None
    value = line[start:i - 1]
    suffix = line[i - 1:]  # starts with '}'
    return prefix, macro_name, value, suffix


def update_variables_tex(values: dict[str, str]) -> tuple[int, int]:
    """Update variables.tex in-place with computed values. Returns (total, updated)."""
    lines = VARIABLES_TEX.read_text(encoding="utf-8").splitlines()
    new_lines = []
    total = 0
    updated = 0

    for line in lines:
        parsed = _parse_newcommand(line)
        if parsed is None:
            new_lines.append(line)
            continue
        total += 1
        prefix, macro_name, current_value, suffix = parsed
        new_value = values.get(macro_name)
        if new_value is not None and new_value != current_value:
            new_lines.append(prefix + macro_name + "}{" + new_value + suffix)
            updated += 1
        else:
            new_lines.append(line)

    VARIABLES_TEX.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    return total, updated


# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------


def _v(values: dict[str, str], key: str) -> int:
    """Read a numeric macro value as int from the all_values dict."""
    raw = values.get(key, "0")
    # Strip the LaTeX-formatted braces around commas: "1{,}234" -> "1234".
    raw = raw.replace("{,}", "").replace(",", "")
    try:
        return int(raw)
    except ValueError:
        return 0


def run_sanity_checks(
    all_values: dict[str, str],
    failure_categories: dict[str, int],
    extra_load_failures: int,
) -> int:
    """Verify cross-cutting invariants between computed macros.

    Returns the number of failing checks (0 = all good).
    """
    failures: list[str] = []

    def check(label: str, lhs: int, rhs: int, lhs_expr: str, rhs_expr: str) -> None:
        ok = lhs == rhs
        status = "OK   " if ok else "FAIL "
        diff = "" if ok else f"  (diff={lhs - rhs:+,})"
        print(f"  [{status}] {label}: {lhs_expr}={lhs:,}  vs  {rhs_expr}={rhs:,}{diff}")
        if not ok:
            failures.append(label)

    # --- Step 1 funnel ---
    in_commits = _v(all_values, "MbFilterInputCommits")
    excl_total = (
        _v(all_values, "MbFilterExclBranches")
        + _v(all_values, "MbFilterExclPreVThree")
        + _v(all_values, "MbFilterExclBot")
        + _v(all_values, "MbFilterExclNoMo")
    )
    retained = _v(all_values, "MbFilterRetained")
    check(
        "Step1: retained + excluded == input",
        retained + excl_total,
        in_commits,
        "retained+excl",
        "input",
    )

    # --- Step 2 totals ---
    all_versions = _v(all_values, "MbListAllClassVersions")
    exp_versions = _v(all_values, "MbListExperimentVersions")
    non_exp_versions = _v(all_values, "MbListNonExperimentVersions")
    check(
        "Step2: experiment + non-experiment == all class versions",
        exp_versions + non_exp_versions,
        all_versions,
        "exp+nonexp",
        "all",
    )

    # --- Step 3 reconciliation: input == produced + failures ---
    # MbCanonInputVersions is a macro alias to MbListExperimentVersions, so use
    # the resolved experiment-version count as the canonical input total.
    produced = _v(all_values, "MbCanonProducedVersions")
    failures_cnt = _v(all_values, "MbCanonFailures")
    check(
        "Step3: produced + failures == experiment versions (input)",
        produced + failures_cnt,
        exp_versions,
        "produced+failures",
        "input",
    )

    # --- Failure-category sum ---
    cat_sum = sum(failure_categories.values())
    check(
        "Failure categories sum == MbCanonFailures",
        cat_sum,
        failures_cnt,
        "sum(categories)",
        "MbCanonFailures",
    )

    # --- Commit-level load failures are fully accounted for in Package load ---
    pkg_load = failure_categories.get("Package load failure", 0)
    if extra_load_failures and pkg_load < extra_load_failures:
        failures.append("Package load category < commit-level load failures")
        print(
            f"  [FAIL ] Package-load category ({pkg_load:,}) "
            f"is smaller than folded-in commit-level failures "
            f"({extra_load_failures:,})"
        )
    else:
        print(
            f"  [OK   ] Package-load category includes folded-in "
            f"commit-level failures ({extra_load_failures:,})"
        )

    return len(failures)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    figure_only_mode = ONLY_STEP2_TIMELINE_FIGURE or _is_figure_only_mode(sys.argv[1:])

    print("=" * 60)
    print("  SAM2026 Paper – Unified Report Generator")
    print("=" * 60)
    if USE_REMOTE:
        print(f"  Mode:        remote (ssh {REMOTE_HOST})")
        print(f"  Remote base: {REMOTE_BASE}")
    else:
        print("  Mode:        local")
        print(f"  Pipeline DB: {PIPELINE_DB}")
        print(f"  Step2 DB:    {STEP2_DB}")
    print(f"  Paper dir:   {PAPER_DIR}")
    if figure_only_mode:
        mode_label = "config flag" if ONLY_STEP2_TIMELINE_FIGURE else "--figure-only"
        print(f"  Mode flag:   {mode_label}")
    print()

    if figure_only_mode:
        print("[FIGS] Figure-only mode: regenerating step2 timeline...")
        _, per_commit_rows = analyze_step2()
        commit_hashes = [r["commit_hash"] for r in per_commit_rows]
        plot_step2_timeline(per_commit_rows, commit_hashes)
        print("\n" + "=" * 60)
        print("  Done! Check figs/step2_classes_timeline.png")
        print("=" * 60)
        return

    all_values: dict[str, str] = {}

    # --- Step 1: Filtering ---
    print("[STEP 1] Commit filtering...")
    step1_values = analyze_step1()
    all_values.update(step1_values)
    print(f"         Retained: {step1_values['MbFilterRetained']}")

    # --- Step 2: Enumeration ---
    print("\n[STEP 2] Class enumeration...")
    step2_values, per_commit_rows = analyze_step2()
    all_values.update(step2_values)
    print(f"         Experiment versions: {step2_values['MbListExperimentVersions']}")
    print(f"         All class mean: {step2_values['MbGrowthAllClassMean']}")

    # --- Step 3: Canonicalization ---
    print("\n[STEP 3] Canonicalization...")
    extra_load_failures = _count_commit_level_load_failures()
    print(f"         Commit-level package-load failures (folded in): {extra_load_failures}")
    step3_values = analyze_step3(extra_load_failures=extra_load_failures)
    all_values.update(step3_values)
    print(f"         Canonical produced: {step3_values['MbCanonProducedVersions']}")
    print(f"         Failures: {step3_values['MbCanonFailures']}")

    # --- Artifact sizes ---
    print("\n[SIZES] Remote artifact sizes...")
    size_values = get_remote_artifact_sizes()
    all_values.update(size_values)
    for k, v in size_values.items():
        print(f"         {k} = {v}")

    # --- Failure analysis ---
    print("\n[FAILURES] Canonicalization failure analysis...")
    failure_categories, total_failures = analyze_failures(
        extra_load_failures=extra_load_failures
    )
    print(f"         Total failures analyzed: {total_failures}")
    print("         Category breakdown:")
    for cat, count in sorted(failure_categories.items(), key=lambda x: -x[1]):
        pct = _pct(count, total_failures)
        print(f"           {cat:40s} {count:>7,}  ({pct:.1f}%)")

    # Map failure categories to LaTeX variable names
    _fail_var_map = {
        "Operator-record restriction": ("MbFailOperatorRestriction", "MbFailOperatorRestrictionPct"),
        "Missing referenced class": ("MbFailMissingClass", "MbFailMissingClassPct"),
        "Replaceable/redeclare conflict": ("MbFailRedeclare", "MbFailRedeclarePct"),
        "Missing base class": ("MbFailMissingBase", "MbFailMissingBasePct"),
        "Canonicalization timeout": ("MbFailTimeout", "MbFailTimeoutPct"),
        "Package load failure": ("MbFailPackageLoad", "MbFailPackageLoadPct"),
    }
    for cat, count in failure_categories.items():
        if cat in _fail_var_map:
            cnt_var, pct_var = _fail_var_map[cat]
            all_values[cnt_var] = _fmt_int(count)
            all_values[pct_var] = f"{_pct(count, total_failures):.1f}"

    # --- Update variables.tex ---
    print("\n[TEX] Updating variables.tex...")
    total_macros, updated_macros = update_variables_tex(all_values)
    print(f"       Total macros: {total_macros}, Updated: {updated_macros}")

    # --- Generate figures ---
    print("\n[FIGS] Generating figures...")
    commit_hashes = [r["commit_hash"] for r in per_commit_rows]
    plot_step2_timeline(per_commit_rows, commit_hashes)
    plot_step2_timeline_v2(per_commit_rows, commit_hashes)

    # --- Sanity checks ---
    print("\n[SANITY] Cross-cutting invariants...")
    n_failed = run_sanity_checks(all_values, failure_categories, extra_load_failures)
    if n_failed:
        print(f"  -> {n_failed} sanity check(s) FAILED")
    else:
        print("  -> all sanity checks passed")

    print("\n" + "=" * 60)
    print("  Done! Check variables.tex and figs/ for updates.")
    print("=" * 60)


if __name__ == "__main__":
    main()

