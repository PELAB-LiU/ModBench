"""Step 1: filter commits and touched Modelica files into SQLite tables."""

import re
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from statistics import median

import git  # GitPython
from tqdm import tqdm


from settings import (
    SourceConfig,
    get_enabled_sources,
    get_connection,
    init_database,
    format_duration,
    start_run_log,
    set_run_values,
    update_run_duration,
    COMMIT_AGE_LIMIT_YEARS,
    BOT_NAME_PATTERNS,
    BOT_EMAIL_PATTERNS,
    BOT_MESSAGE_PATTERN_STRINGS,
    BOT_MESSAGE_HUMAN_OVERRIDE_STRINGS,
    BOT_DOC_ONLY_PATTERN_STRINGS,
    BOT_DOC_EXTENSIONS,
)

# Compile message patterns once at import time.
BOT_MESSAGE_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE) for p in BOT_MESSAGE_PATTERN_STRINGS
]
BOT_MESSAGE_HUMAN_OVERRIDES: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE) for p in BOT_MESSAGE_HUMAN_OVERRIDE_STRINGS
]
BOT_DOC_ONLY_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE) for p in BOT_DOC_ONLY_PATTERN_STRINGS
]

TagCutoffInfo = tuple[str, int, str]  # (tag_name, authored_timestamp_utc, YYYY-MM-DD)

# ---------------------------------------------------------------------------
# BOT-DETECTION HELPERS
# ---------------------------------------------------------------------------

def _check_author_name(name: str) -> str | None:
    name_lower = name.lower()
    for pattern in BOT_NAME_PATTERNS:
        if pattern.lower() in name_lower:
            return f"author name contains '{pattern}'"
    return None


def _check_author_email(email: str) -> str | None:
    email_lower = email.lower()
    for pattern in BOT_EMAIL_PATTERNS:
        if pattern.lower() in email_lower:
            return f"author email contains '{pattern}'"
    return None


def _check_commit_message(message: str) -> str | None:
    """Human-intent overrides are evaluated first; if matched the commit is
    always treated as human regardless of any bot pattern that also fires."""
    first_line = message.strip().splitlines()[0] if message.strip() else ""
    for override in BOT_MESSAGE_HUMAN_OVERRIDES:
        if override.search(first_line):
            return None
    for pattern in BOT_MESSAGE_PATTERNS:
        if pattern.search(first_line):
            return f"commit message matches bot pattern '{pattern.pattern}'"
    return None


def _get_changed_extensions(commit: git.Commit) -> set[str]:
    """Return the set of lowercase file extensions touched by this commit."""
    if commit.parents:
        diffs = commit.parents[0].diff(commit)
    else:
        empty_tree = commit.repo.tree("4b825dc642cb6eb9a060e54bf8d69288fbee4904")
        diffs = empty_tree.diff(commit.tree)
    extensions: set[str] = set()
    for diff in diffs:
        for path in filter(None, {diff.b_path, diff.a_path}):
            extensions.add(Path(path).suffix.lower())
    return extensions


def is_bot_commit(commit: git.Commit) -> tuple[bool, str]:
    author_name  = commit.author.name  or ""
    author_email = commit.author.email or ""
    message      = commit.message      or ""

    reason = _check_author_name(author_name)
    if reason:
        return True, reason

    reason = _check_author_email(author_email)
    if reason:
        return True, reason

    reason = _check_commit_message(message)
    if reason:
        first_line = message.strip().splitlines()[0] if message.strip() else ""
        for doc_only_pattern in BOT_DOC_ONLY_PATTERNS:
            if doc_only_pattern.search(first_line):
                non_doc_exts = _get_changed_extensions(commit) - BOT_DOC_EXTENSIONS
                if non_doc_exts:
                    return False, ""
                break
        return True, reason

    return False, ""


# ---------------------------------------------------------------------------
# DATE / TAG CUTOFF HELPERS
# ---------------------------------------------------------------------------

def is_commit_too_old(commit: git.Commit) -> tuple[bool, str]:
    if COMMIT_AGE_LIMIT_YEARS is None:
        return False, ""
    commit_date = datetime.fromtimestamp(commit.authored_date, tz=timezone.utc)
    age_years = (datetime.now(timezone.utc) - commit_date).days / 365.25
    if age_years > COMMIT_AGE_LIMIT_YEARS:
        date_str = commit_date.strftime("%Y-%m-%d")
        return True, f"commit date: {date_str} ({age_years:.1f} years old)"
    return False, ""


def resolve_tag_cutoff(repo: git.Repo, cfg: SourceConfig) -> TagCutoffInfo | None:
    if not cfg.commit_cutoff_tag:
        return None
    tag_name = cfg.commit_cutoff_tag
    tag_ref = next((t for t in repo.tags if t.name == tag_name), None)
    if tag_ref is None:
        print(f"[ERROR] Cutoff tag '{tag_name}' not found in repository: {cfg.repo_path}",
              file=sys.stderr)
        sys.exit(1)
    cutoff_ts   = int(tag_ref.commit.authored_date)
    cutoff_date = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    print(f"[INFO]  Using tag cutoff: {tag_name} (tag commit date: {cutoff_date})")
    return tag_name, cutoff_ts, cutoff_date


def is_commit_before_tag_cutoff(commit: git.Commit, cutoff: TagCutoffInfo) -> tuple[bool, str]:
    tag_name, cutoff_ts, cutoff_date = cutoff
    if int(commit.authored_date) < cutoff_ts:
        return True, f"before tag {tag_name} (tag date: {cutoff_date})"
    return False, ""


# ---------------------------------------------------------------------------
# REPOSITORY HELPERS
# ---------------------------------------------------------------------------

def load_repository(repo_path: Path) -> git.Repo:
    if not repo_path.exists():
        print(f"[ERROR] Repository path does not exist: {repo_path}", file=sys.stderr)
        sys.exit(1)
    try:
        repo = git.Repo(repo_path)
    except git.InvalidGitRepositoryError:
        print(f"[ERROR] Not a valid Git repository: {repo_path}", file=sys.stderr)
        sys.exit(1)
    print(f"[INFO]  Loaded repository: {repo_path}")
    return repo


def _normalise_branch_name(ref_name: str) -> str:
    return ref_name.removeprefix("origin/")


def collect_commits(
    repo: git.Repo,
    excluded_branches: list[str],
) -> tuple[list[tuple[git.Commit, list[str]]], list[tuple[git.Commit, list[str]]], int]:
    """
    Walk all remote refs.  Skip excluded branches (counting their unique SHAs),
    then return:
      - included: deduplicated (commit, [branches]) list from non-excluded branches
      - excluded_only_commits: (commit, [branches]) for SHAs that appear *only* in excluded branches
      - excluded_only count
    """
    excluded = {b.lower() for b in excluded_branches}
    sha_to_branches: dict[str, set[str]]   = {}
    sha_to_commit:   dict[str, git.Commit] = {}
    excluded_sha_to_branches: dict[str, set[str]]   = {}
    excluded_sha_to_commit:   dict[str, git.Commit] = {}

    for ref in repo.remote().refs:
        branch_name = _normalise_branch_name(ref.name)
        if branch_name.lower() in excluded:
            print(f"[INFO]  Skipping excluded branch: {branch_name}")
            for commit in repo.iter_commits(ref):
                sha = commit.hexsha
                if sha not in excluded_sha_to_branches:
                    excluded_sha_to_branches[sha] = set()
                    excluded_sha_to_commit[sha] = commit
                excluded_sha_to_branches[sha].add(branch_name)
            continue
        for commit in repo.iter_commits(ref):
            sha = commit.hexsha
            if sha not in sha_to_branches:
                sha_to_branches[sha] = set()
                sha_to_commit[sha]   = commit
            sha_to_branches[sha].add(branch_name)

    result: list[tuple[git.Commit, list[str]]] = [
        (sha_to_commit[sha], sorted(sha_to_branches[sha]))
        for sha in sha_to_commit
    ]
    # Commits that appear only in excluded branches (not in any included branch)
    excluded_only_shas = set(excluded_sha_to_commit.keys()) - set(sha_to_commit.keys())
    excluded_only_commits: list[tuple[git.Commit, list[str]]] = [
        (excluded_sha_to_commit[sha], sorted(excluded_sha_to_branches[sha]))
        for sha in excluded_only_shas
    ]
    print(f"[INFO]  Unique commits across included branches: {len(result):,}")
    print(f"[INFO]  Unique commits only in excluded branches: {len(excluded_only_commits):,}")
    return result, excluded_only_commits, len(excluded_only_commits)


# ---------------------------------------------------------------------------
# .MO FILE EXTRACTION
# ---------------------------------------------------------------------------

def _get_mo_files(commit: git.Commit) -> list[str]:
    """Return deduplicated .mo file paths touched by *commit*."""
    if commit.parents:
        diffs = commit.parents[0].diff(commit)
    else:
        empty_tree = commit.repo.tree("4b825dc642cb6eb9a060e54bf8d69288fbee4904")
        diffs = empty_tree.diff(commit.tree)
    mo_files: list[str] = []
    for diff in diffs:
        for path in filter(None, {diff.b_path, diff.a_path}):
            if path.endswith(".mo"):
                mo_files.append(path)
    return list(dict.fromkeys(mo_files))


# ---------------------------------------------------------------------------
# SINGLE-PASS PROCESSING
# ---------------------------------------------------------------------------

def process_commits_and_files(
    commit_branch_pairs: list[tuple[git.Commit, list[str]]],
    excluded_branch_commits: list[tuple[git.Commit, list[str]]],
    tag_cutoff: TagCutoffInfo | None,
) -> tuple[list[dict], list[dict], int]:
    """
    Classify every commit AND extract .mo file paths for included commits in
    one loop — reusing the git.Commit object already in memory.

    Returns
    -------
    commit_records : list[dict]  one row per commit (all commits)
    file_records   : list[dict]  one row per (commit, .mo file), included commits only
    touching_mo    : int         included commits that touched ≥1 .mo file
    """
    commit_records: list[dict] = []
    file_records:   list[dict] = []
    touching_mo = 0

    # First, add excluded-branch commits
    for commit, branches in excluded_branch_commits:
        sha        = commit.hexsha
        email      = commit.author.email or ""
        msg        = commit.message.strip()
        branch_str = "; ".join(branches)
        commit_records.append({
            "commit_hash": sha, "author_email": email,
            "branches": branch_str, "commit_message": msg,
            "excluded": "true", "exclusion_reason": "excluded branch",
        })

    for commit, branches in tqdm(
        commit_branch_pairs, desc="Analysing commits", unit="commit"
    ):
        sha        = commit.hexsha
        email      = commit.author.email or ""
        msg        = commit.message.strip()
        branch_str = "; ".join(branches)

        # ── exclusion checks ──────────────────────────────────────────────
        if tag_cutoff is not None:
            before_tag, reason = is_commit_before_tag_cutoff(commit, tag_cutoff)
            if before_tag:
                commit_records.append({
                    "commit_hash": sha, "author_email": email,
                    "branches": branch_str, "commit_message": msg,
                    "excluded": "true", "exclusion_reason": reason,
                })
                continue

        too_old, reason = is_commit_too_old(commit)
        if too_old:
            commit_records.append({
                "commit_hash": sha, "author_email": email,
                "branches": branch_str, "commit_message": msg,
                "excluded": "true", "exclusion_reason": reason,
            })
            continue

        bot_flag, bot_reason = is_bot_commit(commit)
        if bot_flag:
            commit_records.append({
                "commit_hash": sha, "author_email": email,
                "branches": branch_str, "commit_message": msg,
                "excluded": "true", "exclusion_reason": bot_reason,
            })
            continue

        # ── .mo extraction (included commits only, same object in memory) ─
        mo_files = _get_mo_files(commit)
        if not mo_files:
            commit_records.append({
                "commit_hash": sha, "author_email": email,
                "branches": branch_str, "commit_message": msg,
                "excluded": "true", "exclusion_reason": "no .mo files",
            })
            continue

        commit_records.append({
            "commit_hash": sha, "author_email": email,
            "branches": branch_str, "commit_message": msg,
            "excluded": "false", "exclusion_reason": "",
        })
        touching_mo += 1
        for path in mo_files:
            file_records.append({"commit_hash": sha, "mo_file_path": path})

    return commit_records, file_records, touching_mo


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# OUTPUT HELPERS
# ---------------------------------------------------------------------------

def write_step1_tables(
    source_name: str,
    commit_records: list[dict],
    file_records: list[dict],
) -> None:
    """Replace Step 1 data for one source in SQLite."""
    with get_connection() as conn:
        conn.execute("DELETE FROM step1_commits WHERE source_name = ?", (source_name,))
        conn.execute("DELETE FROM step1_files WHERE source_name = ?", (source_name,))

        conn.executemany(
            """
            INSERT INTO step1_commits
            (source_name, commit_hash, author_email, branches, commit_message, excluded, exclusion_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    source_name,
                    r["commit_hash"],
                    r["author_email"],
                    r["branches"],
                    r["commit_message"],
                    1 if r["excluded"] == "true" else 0,
                    r["exclusion_reason"],
                )
                for r in commit_records
            ],
        )

        conn.executemany(
            """
            INSERT INTO step1_files
            (source_name, commit_hash, mo_file_path)
            VALUES (?, ?, ?)
            """,
            [
                (
                    source_name,
                    r["commit_hash"],
                    r["mo_file_path"],
                )
                for r in file_records
            ],
        )


def print_summary(
    source_name:           str,
    run_id:                int,
    commit_records:        list[dict],
    file_records:          list[dict],
    excluded_branch_count: int,
    touching_mo:           int,
    start_time:            float,
) -> None:
    """Print one merged summary covering both commit filtering and file extraction."""
    scoped_total   = len(commit_records)
    excluded_count = sum(1 for r in commit_records if r["excluded"] == "true")

    bot_count = sum(1 for r in commit_records
                    if r["excluded"] == "true" and "bot" in r["exclusion_reason"].lower())
    tag_count = sum(1 for r in commit_records
                    if r["excluded"] == "true" and "before tag" in r["exclusion_reason"].lower())
    old_count = sum(1 for r in commit_records
                    if r["excluded"] == "true" and "commit date" in r["exclusion_reason"].lower())

    included_count       = scoped_total - excluded_count
    excluded_no_mo       = sum(1 for r in commit_records if r["excluded"] == "true" and "no .mo files" in r["exclusion_reason"].lower())
    total_unique_commits = scoped_total + excluded_branch_count
    total_excluded       = excluded_count + excluded_branch_count
    total_file_rows      = len(file_records)
    commit_mo_pairs      = total_file_rows

    files_per_commit: dict[str, int] = {}
    for row in file_records:
        commit_hash = row["commit_hash"]
        files_per_commit[commit_hash] = files_per_commit.get(commit_hash, 0) + 1

    pair_mean = (commit_mo_pairs / touching_mo) if touching_mo else 0.0
    pair_median_raw = median(files_per_commit.values()) if files_per_commit else 0
    if isinstance(pair_median_raw, float) and pair_median_raw.is_integer():
        pair_median = str(int(pair_median_raw))
    else:
        pair_median = f"{pair_median_raw}"

    def pct(n: int) -> float:
        return (n / total_unique_commits * 100) if total_unique_commits else 0.0

    commit_candidates_after_other_filters = touching_mo + excluded_no_mo

    def pct_included(n: int) -> float:
        return (n / commit_candidates_after_other_filters * 100) if commit_candidates_after_other_filters else 0.0

    summary_text = (
        f"\n{'=' * 52}\n"
        f"  Summary\n"
        f"{'=' * 52}\n"
        f"  Total unique commits    : {total_unique_commits:>6,}\n"
        f"  Excluded by branch      : {excluded_branch_count:>6,}  ({pct(excluded_branch_count):.1f} %)\n"
        f"  Excluded (bot)          : {bot_count:>6,}  ({pct(bot_count):.1f} %)\n"
        f"  Excluded (before tag)   : {tag_count:>6,}  ({pct(tag_count):.1f} %)\n"
        f"  Excluded (old by age)   : {old_count:>6,}  ({pct(old_count):.1f} %)\n"
        f"  Excluded (no .mo files) : {excluded_no_mo:>6,}  ({pct_included(excluded_no_mo):.1f} %)\n"
        f"  Total excluded          : {total_excluded:>6,}  ({pct(total_excluded):.1f} %)\n"
        f"  Total included          : {included_count:>6,}  ({pct(included_count):.1f} %)\n"
        f"  {'─' * 46}\n"
        f"  Total (commit, file)    : {commit_mo_pairs:>6,}\n"
        f"{'=' * 52}"
    )

    print(summary_text)

    generated = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    elapsed = time.time() - start_time
    duration_str = format_duration(elapsed)

    set_run_values(
        run_id,
        {
            r"\StepOneInputUniqueCommits": total_unique_commits,
            r"\StepOneExcludedBranchesCount": excluded_branch_count,
            r"\StepOneExcludedBranchesPct": f"{pct(excluded_branch_count):.1f}",
            r"\StepOneExcludedBotCount": bot_count,
            r"\StepOneExcludedBotPct": f"{pct(bot_count):.1f}",
            r"\StepOneExcludedPreVThreeCount": tag_count,
            r"\StepOneExcludedPreVThreePct": f"{pct(tag_count):.1f}",
            r"\StepOneExcludedNoMoCount": excluded_no_mo,
            r"\StepOneExcludedNoMoPct": f"{pct_included(excluded_no_mo):.1f}",
            r"\StepOneTotalExcludedCount": total_excluded,
            r"\StepOneTotalExcludedPct": f"{pct(total_excluded):.1f}",
            r"\TotalCommitsFiltered": included_count,
            r"\StepOneCommitMoPairs": commit_mo_pairs,
            r"\StepOneCommitMoPairsMean": f"{pair_mean:.2f}",
            r"\StepOneCommitMoPairsMedian": pair_median,
        },
    )
    update_run_duration(run_id, duration_str)
    print(f"[INFO]  Structured summary saved (run_id={run_id}, source={source_name}, duration={duration_str})")


# ---------------------------------------------------------------------------
# PER-SOURCE PROCESSING
# ---------------------------------------------------------------------------

def process_source(cfg: SourceConfig, start_time: float) -> None:
    """Run Step 1 (filter commits + extract .mo file touches) for a single source."""
    run_id = start_run_log(
        cfg.name,
        "step1",
        run_settings={
            "commit_age_limit_years": COMMIT_AGE_LIMIT_YEARS,
            "excluded_branches": cfg.excluded_branches,
            "default_branch": cfg.default_branch,
            "commit_cutoff_tag": cfg.commit_cutoff_tag,
        },
    )

    print(f"\n{'─' * 52}")
    print(f"  Source: {cfg.name}")
    print(f"{'─' * 52}")

    repo = load_repository(cfg.repo_path)
    tag_cutoff = resolve_tag_cutoff(repo, cfg)

    commit_branch_pairs, excluded_branch_commits, excluded_branch_count = collect_commits(repo, cfg.excluded_branches)

    # Single pass: classify commits + extract .mo files for included commits
    commit_records, file_records, touching_mo = process_commits_and_files(
        commit_branch_pairs, excluded_branch_commits, tag_cutoff
    )

    write_step1_tables(cfg.name, commit_records, file_records)
    print(f"[INFO]  Stored step1_commits rows: {len(commit_records):,}")
    print(f"[INFO]  Stored step1_files rows  : {len(file_records):,}")

    print_summary(
        cfg.name,
        run_id,
        commit_records, file_records,
        excluded_branch_count, touching_mo,
        start_time,
    )


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def main() -> None:
    """Run Step 1 for all enabled sources."""
    start_time = time.time()
    
    init_database()
    sources = get_enabled_sources()

    print("=" * 52)
    print("  Step 1 – Filter Commits & Files")
    print("=" * 52)
    print(f"[INFO]  Enabled sources: {', '.join(s.name for s in sources)}")

    if not sources:
        print("[WARN]  No sources are enabled in settings.py — nothing to do.")
        return

    for cfg in sources:
        process_source(cfg, start_time)

    print(f"\n[INFO]  Step 1 complete for {len(sources)} source(s).")



if __name__ == "__main__":
    main()

