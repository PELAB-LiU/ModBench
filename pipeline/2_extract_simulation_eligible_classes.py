"""
Step 2: Enumerate simulation-eligible classes for each commit and store in SQLite.

For each enabled source and each commit from Step 1:
1. Checkout the commit in the Git repository
2. Load the Modelica library using OpenModelica
3. Enumerate all classes and determine experiment status (isExperiment=true)
4. Store results in the step2_classes table

This step does NOT build canonical models; that is handled by Step 3.

Features:
- Multi-process commit queue with isolated git worktrees per worker
- Single DB writer process for safe SQLite access
- File-based class index cache for fast re-enumeration
- Safe to interrupt/resume (incremental atomic per-commit output)

Usage (from the project root):
    python dataset_creator/2_extract_simulation_eligible_classes.py

Input (per source):
    SQLite table step1_commits

Output (per source):
    SQLite tables step2_classes, step2_enumeration_progress
"""

from __future__ import annotations

import contextlib
import io
import json
import multiprocessing as mp
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, NamedTuple

import git
from tqdm import tqdm

SETTINGS_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = SETTINGS_DIR.parent

from settings import (  # noqa: E402
    SourceConfig,
    get_connection,
    get_step2_classes_connection,
    get_enabled_sources,
    get_setting_int,
    init_database,
    init_step2_classes_db,
    format_duration,
    start_run_log,
    update_run_duration,
)

_STEP_NUMBER = 2


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------------
# MULTI-PROCESS DB ABSTRACTION
# ---------------------------------------------------------------------------

class DBOp(NamedTuple):
    """A single DB operation to be executed by the DB writer process."""

    kind: Literal["enumeration_progress", "classes_batch"]
    payload: dict[str, Any]


class ProgressEvent(NamedTuple):
    """A progress event emitted by the DB writer after a commit is finalized."""

    kind: Literal["commit_done"]
    payload: dict[str, Any]


class DBSink:
    """Abstract DB sink used by both single- and multi-process implementations."""


    def upsert_enumeration_progress(
        self,
        *,
        source_name: str,
        commit_hash: str,
        status: str,
        enumerated_classes_count: int,
    ) -> None:
        raise NotImplementedError

    def insert_enumerated_classes_batch(
        self,
        *,
        rows: list[tuple],
    ) -> None:
        raise NotImplementedError


class DirectDBSink(DBSink):
    """Writes to SQLite directly (single-process mode)."""


    def upsert_enumeration_progress(
        self,
        *,
        source_name: str,
        commit_hash: str,
        status: str,
        enumerated_classes_count: int,
    ) -> None:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO step2_enumeration_progress
                (source_name, commit_hash, status, enumerated_classes_count, last_updated_utc)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_name, commit_hash) DO UPDATE SET
                    status = excluded.status,
                    enumerated_classes_count = excluded.enumerated_classes_count,
                    last_updated_utc = excluded.last_updated_utc
                """,
                (source_name, commit_hash, status, enumerated_classes_count, utc_now()),
            )

    def insert_enumerated_classes_batch(
        self,
        *,
        rows: list[tuple],
    ) -> None:
        if not rows:
            return
        with get_step2_classes_connection() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO step2_classes
                (source_name, commit_hash, class_name, is_experiment)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )


class QueueDBSink(DBSink):
    """Enqueues DB operations for a single writer process."""

    def __init__(self, op_queue: "mp.Queue[DBOp]"):
        self._q = op_queue


    def upsert_enumeration_progress(
        self,
        *,
        source_name: str,
        commit_hash: str,
        status: str,
        enumerated_classes_count: int,
    ) -> None:
        self._q.put(
            DBOp(
                "enumeration_progress",
                {
                    "source_name": source_name,
                    "commit_hash": commit_hash,
                    "status": status,
                    "enumerated_classes_count": int(enumerated_classes_count),
                },
            )
        )

    def insert_enumerated_classes_batch(
        self,
        *,
        rows: list[tuple],
    ) -> None:
        if not rows:
            return
        self._q.put(DBOp("classes_batch", {"rows": rows}))


# ---------------------------------------------------------------------------
# CLASS INDEX CACHE (JSON files on disk for fast re-enumeration)
# ---------------------------------------------------------------------------

def _cache_dir_for_source(source_name: str) -> Path:
    return SETTINGS_DIR / "cache" / "step2_class_index" / source_name


def _read_class_index_cache(source_name: str, commit_hash: str) -> list[tuple[str, bool]] | None:
    path = _cache_dir_for_source(source_name) / f"{commit_hash}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        items = data.get("classes")
        if not isinstance(items, list):
            return None
        out: list[tuple[str, bool]] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            name = str(it.get("name", "")).strip()
            if not name:
                continue
            out.append((name, bool(it.get("is_experiment", False))))
        return out or None
    except Exception:
        return None


def _write_class_index_cache(source_name: str, commit_hash: str, classes: list[tuple[str, bool]]) -> None:
    try:
        out_dir = _cache_dir_for_source(source_name)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{commit_hash}.json"
        payload = {
            "source_name": source_name,
            "commit_hash": commit_hash,
            "generated_at_utc": utc_now(),
            "classes": [{"name": n, "is_experiment": bool(f)} for n, f in classes],
        }
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# OMC HELPERS
# ---------------------------------------------------------------------------

def check_omc_available() -> bool:
    return shutil.which("omc") is not None


def init_omc_session():
    try:
        from OMPython import OMCSessionZMQ
        return OMCSessionZMQ()
    except ImportError:
        print("[ERROR] OMPython not installed. Install with: pip install OMPython")
        return None
    except Exception as e:
        print(f"[ERROR] Failed to initialize OMC session: {e}")
        return None


def load_repository(repo_path: Path, source_name: str) -> tuple[git.Repo, Path]:
    script_relative_source = (SETTINGS_DIR / ".." / "source" / source_name).resolve()
    primary_path = script_relative_source
    fallback_path = repo_path.resolve()

    for candidate in (primary_path, fallback_path):
        if not candidate.exists():
            continue
        try:
            return git.Repo(candidate), candidate
        except git.InvalidGitRepositoryError:
            continue

    print(f"[ERROR] Not a valid Git repository: {primary_path}", file=sys.stderr)
    if fallback_path != primary_path:
        print(f"[ERROR] Fallback path checked   : {fallback_path}", file=sys.stderr)
    sys.exit(1)


class OMCOutputSuppressor:
    """Suppresses stdout/stderr from OMPython calls to keep the console clean."""
    _BENIGN_STDERR_LINES = {
        "Result of 'getErrorString()' cannot be parsed!",
    }

    @contextlib.contextmanager
    def suppress_omc_output(self):
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        try:
            sys.stdout = stdout_buffer
            sys.stderr = stderr_buffer
            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr



def load_modelica_for_commit(omc, suppressor: OMCOutputSuppressor, repo_path: Path, load_targets: list[tuple[str, list[str]]]) -> bool:
    """Load Modelica libraries for the currently checked-out commit.

    Uses explicit ``loadFile`` calls with ``uses=false`` so that OMC does not
    attempt to resolve inter-library dependencies automatically.  The load
    order is determined by the load_targets list read from the source's
    package_file column in step1_sources.

    Each candidate path is resolved in this order: absolute path,
    ``<repo_root>/<candidate>``, then ``<SAM2026_ROOT>/<candidate>``. The
    last fallback lets a source pull in dependencies (e.g. MSL) that live
    under ``source/<other_lib>/`` instead of inside its own worktree.
    """
    try:
        with suppressor.suppress_omc_output():
            omc.sendExpression("clear()")

            repo_root = repo_path.resolve()
            last_pkg_name = load_targets[-1][0] if load_targets else ""
            main_pkg_loaded = False

            for pkg_name, candidates in load_targets:
                pkg_file: Path | None = None
                for candidate in candidates:
                    for base in (repo_root, SETTINGS_DIR):
                        p = base / candidate
                        if p.is_file():
                            pkg_file = p
                            break
                    if pkg_file is not None:
                        break

                if pkg_file is None:
                    if pkg_name == last_pkg_name:
                        return False
                    continue

                load_ok = omc.sendExpression(
                    f'loadFile("{pkg_file}", uses=false)'
                )
                omc.sendExpression("getErrorString()")

                if not bool(load_ok):
                    if pkg_name == last_pkg_name:
                        return False

                if pkg_name == last_pkg_name:
                    main_pkg_loaded = bool(load_ok)

        return main_pkg_loaded
    except Exception:
        return False


def _main_package_name(cfg: SourceConfig) -> str:
    """Top-level Modelica package to enumerate (last entry in load_targets)."""
    return cfg.load_targets[-1][0] if cfg.load_targets else cfg.name


def get_all_classes_with_experiment_status(
    omc,
    suppressor: OMCOutputSuppressor,
    main_package_name: str,
) -> list[tuple[str, bool]]:
    """Return ``(class_name, is_experiment)`` for every class under
    *main_package_name* (the top-level Modelica package being mined)."""
    try:
        with suppressor.suppress_omc_output():
            all_classes = omc.sendExpression(
                f"getClassNames({main_package_name}, recursive=true)"
            )
            omc.sendExpression("getErrorString()")

        if not isinstance(all_classes, (list, tuple)):
            return []

        out: list[tuple[str, bool]] = []
        for class_name in all_classes:
            if not isinstance(class_name, str) or not class_name.strip():
                continue
            try:
                with suppressor.suppress_omc_output():
                    is_exp = omc.sendExpression(f"isExperiment({class_name})")
                out.append((class_name, bool(is_exp)))
            except Exception:
                out.append((class_name, False))

        return out
    except Exception:
        return []


# ---------------------------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------------------------

def read_commit_hashes_for_source(source_name: str, dry_run_limit: int | None) -> list[str]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT commit_hash
            FROM step1_commits
            WHERE source_name = ?
              AND excluded = 0
            ORDER BY commit_hash
            """,
            (source_name,),
        ).fetchall()
    commits = [str(r["commit_hash"]) for r in rows]
    if dry_run_limit and dry_run_limit > 0:
        return commits[:dry_run_limit]
    return commits


def read_enumeration_progress(source_name: str) -> dict[str, str]:
    """Return {commit_hash: status} for all enumerated commits."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT commit_hash, status
            FROM step2_enumeration_progress
            WHERE source_name = ?
            """,
            (source_name,),
        ).fetchall()
    return {str(r["commit_hash"]): str(r["status"]) for r in rows}


def _upsert_enumeration_progress(
    source_name: str,
    commit_hash: str,
    status: str,
    enumerated_classes_count: int,
    *,
    db: DBSink | None = None,
) -> None:
    (db or DirectDBSink()).upsert_enumeration_progress(
        source_name=source_name,
        commit_hash=commit_hash,
        status=status,
        enumerated_classes_count=enumerated_classes_count,
    )


def _insert_enumerated_classes_batch(
    source_name: str,
    commit_hash: str,
    classes: list[tuple[str, bool]],
    *,
    db: DBSink | None = None,
) -> None:
    if not classes:
        return
    rows = [(source_name, commit_hash, name, 1 if is_exp else 0) for name, is_exp in classes]
    (db or DirectDBSink()).insert_enumerated_classes_batch(rows=rows)


def prompt_continue(remaining: int, total: int) -> bool:
    if remaining <= 0:
        return False
    while True:
        answer = input(
            f"[PROMPT] Step 2 has {total - remaining:,} processed and {remaining:,} remaining commits. "
            "Continue processing remaining commits? [y/N]: "
        ).strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"", "n", "no"}:
            return False
        print("Please answer 'y' or 'n'.")


# ---------------------------------------------------------------------------
# PARALLEL INFRASTRUCTURE
# ---------------------------------------------------------------------------

def _auto_worker_count(requested: int | None) -> int:
    """Auto-tune worker count based on CPU cores."""
    if requested is not None and requested > 0:
        return requested
    cpu = os.cpu_count() or 2
    # Heuristic: OMC + git are heavy; avoid fully oversubscribing by default,
    # but allow more parallelism on larger machines.
    # Example: cpu=16 -> workers=8 by default.
    return max(1, min(cpu - 1, 8))


def _ensure_clean_worktree_base(repo: git.Repo) -> None:
    try:
        repo.git.worktree("prune")
    except Exception:
        pass


def _cleanup_stale_worktrees(repo_path: Path, source_name: str) -> None:
    """Remove stale worktree directories that point to invalid git repos."""
    wt_base = SETTINGS_DIR / "worktrees" / "step2" / source_name
    if not wt_base.exists():
        return

    repo = git.Repo(repo_path)
    _ensure_clean_worktree_base(repo)

    removed = 0
    for wt_dir in sorted(wt_base.iterdir()):
        if not wt_dir.is_dir():
            continue
        # Check if the worktree's .git file points to a valid location
        dot_git = wt_dir / ".git"
        if dot_git.exists():
            try:
                _ = git.Repo(wt_dir)
                # Also verify a simple git command works
                git.Repo(wt_dir).git.status()
            except Exception:
                shutil.rmtree(wt_dir, ignore_errors=True)
                removed += 1
        else:
            shutil.rmtree(wt_dir, ignore_errors=True)
            removed += 1

    if removed:
        _ensure_clean_worktree_base(repo)
        print(f"[INFO]  Removed {removed} stale worktree(s) in {wt_base}")


def _clean_worktree(repo: git.Repo) -> None:
    """Remove untracked leftovers so the worktree matches the checked-out commit.

    Worktrees are reused across thousands of commits, and ``checkout --force``
    only updates *tracked* files. Every file that existed in a previously
    visited revision but not in the current one therefore survives as an
    untracked leftover, and the tree slowly becomes a union of many revisions.
    Left in place those leftovers can collide with the checked-out revision --
    a stale ``Foo/`` package directory next to the revision's ``Foo.mo``, say --
    and make ``loadFile`` on the main package fail for an otherwise good commit.
    """
    repo.git.clean("-xdff")


def _create_or_reset_worktree(
    *,
    repo_path: Path,
    source_name: str,
    worker_id: int,
    default_branch: str,
) -> Path:
    """
    Create (or reuse) an isolated git worktree for a worker.
    We reuse the directory if it already exists, but always force-checkout per commit.
    """
    repo = git.Repo(repo_path)
    _ensure_clean_worktree_base(repo)

    wt_base = SETTINGS_DIR / "worktrees" / "step2" / source_name
    wt_base.mkdir(parents=True, exist_ok=True)
    wt_path = wt_base / f"worker_{worker_id}"

    if wt_path.exists():
        # Best-effort: if worktree is already registered, keep it; otherwise remove dir.
        try:
            _ = git.Repo(wt_path)
        except Exception:
            shutil.rmtree(wt_path, ignore_errors=True)

    if not wt_path.exists():
        # Detached worktree based on default branch (we'll checkout commits).
        repo.git.worktree("add", "--detach", str(wt_path), default_branch)

    return wt_path


def _db_writer_loop(
    op_queue: "mp.Queue[DBOp]",
    progress_queue: "mp.Queue[ProgressEvent]",
    stop_event: "mp.Event",
) -> None:
    """
    Single SQLite writer process for Step 2.
    This avoids SQLite multi-writer contention and ensures frequent flushes so partial results
    survive interruption.
    """
    sink = DirectDBSink()

    while True:
        if stop_event.is_set() and op_queue.empty():
            break
        try:
            op = op_queue.get(timeout=0.25)
        except Exception:
            continue

        if op.kind == "enumeration_progress":
            sink.upsert_enumeration_progress(**op.payload)
            # Emit a progress event only on completion.
            if str(op.payload.get("status")) == "complete":
                progress_queue.put(ProgressEvent("commit_done", dict(op.payload)))
            continue
        if op.kind == "classes_batch":
            rows = op.payload.get("rows") or []
            if rows:
                sink.insert_enumerated_classes_batch(rows=rows)
            continue


def _worker_loop(
    *,
    cfg: SourceConfig,
    repo_path: Path,
    default_branch: str,
    commit_queue: "mp.Queue[str]",
    op_queue: "mp.Queue[DBOp]",
    stop_event: "mp.Event",
    worker_id: int,
) -> None:
    """Worker process: checkout commits, enumerate classes, send DB ops via queue."""
    wt_path = _create_or_reset_worktree(
        repo_path=repo_path,
        source_name=cfg.name,
        worker_id=worker_id,
        default_branch=default_branch,
    )
    repo = git.Repo(wt_path)

    omc = init_omc_session()
    if omc is None:
        return
    db = QueueDBSink(op_queue)

    while not stop_event.is_set():
        try:
            commit_hash = commit_queue.get(timeout=0.25)
        except Exception:
            continue

        if not commit_hash:
            break  # Sentinel: no more commits

        db.upsert_enumeration_progress(
            source_name=cfg.name,
            commit_hash=commit_hash,
            status="processing",
            enumerated_classes_count=0,
        )

        logger = OMCOutputSuppressor()
        checkout_target = default_branch if commit_hash == "HEAD" else commit_hash

        try:
            repo.git.checkout(checkout_target, force=True)
            _clean_worktree(repo)
        except git.GitCommandError as e:
            db.upsert_enumeration_progress(
                source_name=cfg.name,
                commit_hash=commit_hash,
                status="complete",
                enumerated_classes_count=0,
            )
            continue

        if not load_modelica_for_commit(omc, logger, wt_path, cfg.load_targets):
            db.upsert_enumeration_progress(
                source_name=cfg.name,
                commit_hash=commit_hash,
                status="complete",
                enumerated_classes_count=0,
            )
            continue

        # Try cache first
        cached = _read_class_index_cache(cfg.name, commit_hash)
        if cached:
            classes_with_flags = cached
        else:
            classes_with_flags = get_all_classes_with_experiment_status(
                omc, logger, _main_package_name(cfg)
            )
            if classes_with_flags:
                _write_class_index_cache(cfg.name, commit_hash, classes_with_flags)

        if classes_with_flags:
            _insert_enumerated_classes_batch(cfg.name, commit_hash, classes_with_flags, db=db)

        db.upsert_enumeration_progress(
            source_name=cfg.name,
            commit_hash=commit_hash,
            status="complete",
            enumerated_classes_count=len(classes_with_flags),
        )


def _process_source_parallel(
    *,
    cfg: SourceConfig,
    omc_version: str,
    start_time: float,
    repo_path: Path,
    default_branch: str,
    remaining_commits: list[str],
    total_commits: int,
    processed_initial: int,
    workers: int,
    run_id: int,
    commit_hashes: list[str],
) -> None:
    """Orchestrate parallel class enumeration using multi-process commit queue."""
    ctx = mp.get_context("spawn")
    commit_queue: "mp.Queue[str]" = ctx.Queue()
    op_queue: "mp.Queue[DBOp]" = ctx.Queue()
    progress_queue: "mp.Queue[ProgressEvent]" = ctx.Queue()
    stop_event = ctx.Event()

    for c in remaining_commits:
        commit_queue.put(c)
    # Sentinels to stop workers cleanly.
    for _ in range(workers):
        commit_queue.put("")

    writer = ctx.Process(target=_db_writer_loop, args=(op_queue, progress_queue, stop_event), daemon=True)
    writer.start()

    procs: list[mp.Process] = []
    for wid in range(workers):
        p = ctx.Process(
            target=_worker_loop,
            kwargs={
                "cfg": cfg,
                "repo_path": repo_path,
                "default_branch": default_branch,
                "commit_queue": commit_queue,
                "op_queue": op_queue,
                "stop_event": stop_event,
                "worker_id": wid,
            },
            daemon=True,
        )
        p.start()
        procs.append(p)

    pbar = tqdm(
        total=total_commits,
        initial=processed_initial,
        desc="Enumerating classes",
        unit="commit",
    )

    completed = processed_initial
    interrupted = False
    try:
        while completed < processed_initial + len(remaining_commits):
            try:
                ev = progress_queue.get(timeout=0.25)
            except Exception:
                # If all workers died, we should stop waiting.
                if all(not p.is_alive() for p in procs):
                    break
                continue

            if ev.kind == "commit_done":
                completed += 1
                pbar.update(1)
    except KeyboardInterrupt:
        stop_event.set()
        interrupted = True
        print("\n[WARN]  Interrupted by user. Flushing queued DB writes...")
    finally:
        stop_event.set()
        for p in procs:
            p.join(timeout=5)
        writer.join(timeout=5)
        pbar.close()

    elapsed = time.time() - start_time
    update_run_duration(run_id, format_duration(elapsed))

    # Count totals
    with get_step2_classes_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT commit_hash) AS enumerated_commits,
                   COUNT(*) AS total_classes,
                   SUM(CASE WHEN is_experiment = 1 THEN 1 ELSE 0 END) AS experiment_classes
            FROM step2_classes
            WHERE source_name = ?
            """,
            (cfg.name,),
        ).fetchone()

    enumerated_commits = int(row["enumerated_commits"] or 0)
    total_classes = int(row["total_classes"] or 0)
    experiment_classes = int(row["experiment_classes"] or 0)

    summary_text = (
        f"Step 2 - Extract Simulation-Eligible Classes Summary\n"
        f"Generated: {utc_now()}\n\n"
        f"OMC version             : {omc_version}\n"
        f"Commits enumerated      : {enumerated_commits:,} / {len(commit_hashes):,}\n"
        f"Total class entries     : {total_classes:,}\n"
        f"Experiment classes      : {experiment_classes:,}\n"
        f"Workers                 : {workers}\n"
        f"Interrupted             : {interrupted}\n"
    )

    print(summary_text)


# ---------------------------------------------------------------------------
# PER-SOURCE PROCESSING
# ---------------------------------------------------------------------------

def process_source(cfg: SourceConfig, omc_version: str, start_time: float) -> None:
    print(f"\n{'-' * 50}")
    print(f"  Source: {cfg.name}")
    print(f"{'-' * 50}")

    dry_run_limit = get_setting_int(_STEP_NUMBER, "DRY_RUN_LIMIT")
    run_id = start_run_log(
        cfg.name,
        "step2",
        run_settings={
            "dry_run_limit": dry_run_limit,
            "default_branch": cfg.default_branch,
            "omc_version": omc_version,
        },
    )

    repo, effective_repo_path = load_repository(cfg.repo_path, cfg.name)
    _cleanup_stale_worktrees(effective_repo_path, cfg.name)

    commit_hashes = read_commit_hashes_for_source(cfg.name, dry_run_limit)
    progress = read_enumeration_progress(cfg.name)

    processed_commits = {
        commit_hash
        for commit_hash, status in progress.items()
        if status == "complete"
    }
    remaining_commits = [c for c in commit_hashes if c not in processed_commits]

    print(f"[INFO]  Commits to enumerate: {len(commit_hashes):,}")
    print(f"[INFO]  Already processed   : {len(processed_commits):,}")
    print(f"[INFO]  Remaining           : {len(remaining_commits):,}")

    if not remaining_commits:
        print("[INFO]  No remaining commits to process.")
        elapsed = time.time() - start_time
        update_run_duration(run_id, format_duration(elapsed))
        try:
            repo.git.checkout(cfg.default_branch, force=True)
        except Exception:
            pass
        return

    if not prompt_continue(len(remaining_commits), len(commit_hashes)):
        elapsed = time.time() - start_time
        update_run_duration(run_id, format_duration(elapsed))
        print("[INFO]  Step 2 cancelled by user. Existing data kept.")
        try:
            repo.git.checkout(cfg.default_branch, force=True)
        except Exception:
            pass
        return

    # Determine parallel vs sequential mode
    requested_workers = get_setting_int(_STEP_NUMBER, "STEP2_WORKERS")
    workers = _auto_worker_count(requested_workers)
    parallel_enabled = workers > 1 and len(remaining_commits) > 1

    if parallel_enabled:
        print("[INFO]  Step 2 execution   : parallel (multi-process commit queue)")
        print(
            f"[INFO]  Parallel workers   : {workers} "
            f"(override with run_settings(step={_STEP_NUMBER},key='STEP2_WORKERS'))"
        )
        _process_source_parallel(
            cfg=cfg,
            omc_version=omc_version,
            start_time=start_time,
            repo_path=effective_repo_path,
            default_branch=cfg.default_branch,
            remaining_commits=remaining_commits,
            total_commits=len(commit_hashes),
            processed_initial=len(processed_commits),
            workers=workers,
            run_id=run_id,
            commit_hashes=commit_hashes,
        )
        try:
            repo.git.checkout(cfg.default_branch, force=True)
        except Exception:
            pass
        return
    else:
        mode = "sequential" if workers <= 1 else "sequential (parallel disabled: not enough remaining commits)"
        print(f"[INFO]  Step 2 execution   : {mode}")

    # --- Sequential mode ---
    omc = init_omc_session()
    if omc is None:
        sys.exit(1)

    interrupted = False
    pbar = None
    db = DirectDBSink()
    try:
        pbar = tqdm(
            remaining_commits,
            total=len(commit_hashes),
            initial=len(processed_commits),
            desc="Enumerating classes",
            unit="commit",
        )
        for commit_hash in pbar:
            _upsert_enumeration_progress(cfg.name, commit_hash, "processing", 0, db=db)

            suppressor = OMCOutputSuppressor()
            checkout_target = cfg.default_branch if commit_hash == "HEAD" else commit_hash

            try:
                repo.git.checkout(checkout_target, force=True)
                _clean_worktree(repo)
            except git.GitCommandError as e:
                _upsert_enumeration_progress(cfg.name, commit_hash, "complete", 0, db=db)
                continue

            if not load_modelica_for_commit(omc, suppressor, effective_repo_path, cfg.load_targets):
                _upsert_enumeration_progress(cfg.name, commit_hash, "complete", 0, db=db)
                continue

            # Try cache first
            cached = _read_class_index_cache(cfg.name, commit_hash)
            if cached:
                classes_with_flags = cached
            else:
                classes_with_flags = get_all_classes_with_experiment_status(
                    omc, suppressor, _main_package_name(cfg)
                )
                if classes_with_flags:
                    _write_class_index_cache(cfg.name, commit_hash, classes_with_flags)

            if classes_with_flags:
                _insert_enumerated_classes_batch(cfg.name, commit_hash, classes_with_flags, db=db)

            _upsert_enumeration_progress(
                cfg.name, commit_hash, "complete", len(classes_with_flags), db=db
            )

    except KeyboardInterrupt:
        interrupted = True
        print("\n[WARN]  Interrupted by user. Current Step 2 progress has been saved.")

    completed_after = pbar.n if pbar is not None else len(processed_commits)
    elapsed = time.time() - start_time
    duration_str = format_duration(elapsed)
    update_run_duration(run_id, duration_str)

    # Count totals
    with get_step2_classes_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT commit_hash) AS enumerated_commits,
                   COUNT(*) AS total_classes,
                   SUM(CASE WHEN is_experiment = 1 THEN 1 ELSE 0 END) AS experiment_classes
            FROM step2_classes
            WHERE source_name = ?
            """,
            (cfg.name,),
        ).fetchone()

    enumerated_commits = int(row["enumerated_commits"] or 0)
    total_classes = int(row["total_classes"] or 0)
    experiment_classes = int(row["experiment_classes"] or 0)

    summary_text = (
        f"Step 2 - Extract Simulation-Eligible Classes Summary\n"
        f"Generated: {utc_now()}\n\n"
        f"OMC version             : {omc_version}\n"
        f"Commits enumerated      : {enumerated_commits:,} / {len(commit_hashes):,}\n"
        f"Total class entries     : {total_classes:,}\n"
        f"Experiment classes      : {experiment_classes:,}\n"
        f"Interrupted             : {interrupted}\n"
    )

    print(summary_text)

    try:
        repo.git.checkout(cfg.default_branch, force=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def _detect_omc_version() -> str:
    omc = init_omc_session()
    if omc is None:
        return "unknown"
    try:
        version = omc.sendExpression("getVersion()")
        return str(version).strip() or "unknown"
    except Exception:
        return "unknown"


def main() -> None:
    start_time = time.time()

    init_database()
    init_step2_classes_db()
    sources = get_enabled_sources()

    print("=" * 52)
    print("  Step 2 – Extract Simulation-Eligible Classes")
    print("=" * 52)
    print(f"[INFO]  Enabled sources: {', '.join(s.name for s in sources)}")

    if not sources:
        print("[WARN]  No sources are enabled in settings.py - nothing to do.")
        return

    if not check_omc_available():
        print("[ERROR] OpenModelica executable 'omc' is not available on PATH.")
        return

    omc_version = _detect_omc_version()
    print(f"[INFO]  OMC version: {omc_version}")

    for cfg in sources:
        process_source(cfg, omc_version, start_time)

    print(f"\n[INFO]  Step 2 complete for {len(sources)} source(s).")


if __name__ == "__main__":
    main()

