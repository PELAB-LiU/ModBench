"""Step 3: Build canonical representations for simulation-eligible classes.

For each enabled source and selected commit, checks out the commit,
loads the configured Modelica packages with OMC, then runs
``saveTotalModel`` for each candidate class. Results are written to
``step3_classes`` (with ``step3_commit_progress`` and ``step3_failures``)
and canonical files are saved under ``dataset/canonical_models/``.
"""

from __future__ import annotations

import contextlib
import io
import os
import re
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
    get_setting_bool,
    get_setting_int,
    init_database,
    format_duration,
    start_run_log,
    set_run_values,
    update_run_duration,
)

# Step number for this script (used to read settings from run_settings table)
_STEP_NUMBER = 3

import multiprocessing as mp



def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


class DBOp(NamedTuple):
    """A single DB operation to be executed by the DB writer process."""

    kind: Literal["failure", "progress", "classes_batch"]
    payload: dict[str, Any]


class ProgressEvent(NamedTuple):
    """A progress event emitted by the DB writer after a commit is finalized."""

    kind: Literal["commit_done"]
    payload: dict[str, Any]


class DBSink:
    """Abstract DB sink used by both single- and multi-process implementations."""

    def record_failure(
        self,
        *,
        source_name: str,
        commit_hash: str,
        class_name: str,
        failure_type: str,
        compiler_message: str,
    ) -> None:
        raise NotImplementedError

    def upsert_commit_progress(
        self,
        *,
        source_name: str,
        commit_hash: str,
        status: str,
        processed_classes_count: int,
        saved_models_count: int,
        failure_count: int,
    ) -> None:
        raise NotImplementedError

    def insert_classes_batch(self, *, rows: list[tuple]) -> None:
        raise NotImplementedError


class DirectDBSink(DBSink):
    """Writes to SQLite directly (single-process mode)."""

    def record_failure(
        self,
        *,
        source_name: str,
        commit_hash: str,
        class_name: str,
        failure_type: str,
        compiler_message: str,
    ) -> None:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO step3_failures
                (source_name, commit_hash, class_name, failure_type, compiler_message, created_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source_name, commit_hash, class_name, failure_type, compiler_message, utc_now()),
            )

    def upsert_commit_progress(
        self,
        *,
        source_name: str,
        commit_hash: str,
        status: str,
        processed_classes_count: int,
        saved_models_count: int,
        failure_count: int,
    ) -> None:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO step3_commit_progress
                (source_name, commit_hash, status, processed_classes_count, saved_models_count, failure_count, last_updated_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name, commit_hash) DO UPDATE SET
                    status = excluded.status,
                    processed_classes_count = excluded.processed_classes_count,
                    saved_models_count = excluded.saved_models_count,
                    failure_count = excluded.failure_count,
                    last_updated_utc = excluded.last_updated_utc
                """,
                (source_name, commit_hash, status, processed_classes_count, saved_models_count, failure_count, utc_now()),
            )

    def insert_classes_batch(self, *, rows: list[tuple]) -> None:
        if not rows:
            return
        with get_connection() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO step3_classes
                (source_name, commit_hash, class_name, is_experiment, canonical_model_path,
                 error_message, is_inside_sublibraries_list, pilot_match_mode,
                 matched_sublibrary, canonical_produced)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )


class QueueDBSink(DBSink):
    """Enqueues DB operations for a single writer process."""

    def __init__(self, op_queue: "mp.Queue[DBOp]"):
        self._q = op_queue

    def record_failure(
        self,
        *,
        source_name: str,
        commit_hash: str,
        class_name: str,
        failure_type: str,
        compiler_message: str,
    ) -> None:
        self._q.put(
            DBOp(
                "failure",
                {
                    "source_name": source_name,
                    "commit_hash": commit_hash,
                    "class_name": class_name,
                    "failure_type": failure_type,
                    "compiler_message": compiler_message,
                },
            )
        )

    def upsert_commit_progress(
        self,
        *,
        source_name: str,
        commit_hash: str,
        status: str,
        processed_classes_count: int,
        saved_models_count: int,
        failure_count: int,
    ) -> None:
        self._q.put(
            DBOp(
                "progress",
                {
                    "source_name": source_name,
                    "commit_hash": commit_hash,
                    "status": status,
                    "processed_classes_count": int(processed_classes_count),
                    "saved_models_count": int(saved_models_count),
                    "failure_count": int(failure_count),
                },
            )
        )

    def insert_classes_batch(self, *, rows: list[tuple]) -> None:
        if not rows:
            return
        # Note: rows must be picklable; tuples of primitives are fine.
        self._q.put(DBOp("classes_batch", {"rows": rows}))


def _auto_worker_count(requested: int | None) -> int:
    if requested is not None and requested > 0:
        return requested
    cpu = os.cpu_count() or 2
    # Heuristic: OMC + git are heavy; avoid fully oversubscribing by default,
    # but allow more parallelism on larger machines.
    # Example: cpu=16 -> workers=8 by default.
    return max(1, min(cpu - 1, 8))


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


def _normalize_omc_error_string(raw_error) -> str:
    if raw_error is None:
        return ""
    if not isinstance(raw_error, str):
        raw_error = str(raw_error)
    err = raw_error.strip()
    if err in {"", '""', "''"}:
        return ""
    if err.startswith('"') and err.endswith('"'):
        return err[1:-1].strip()
    return err


def _omc_call(omc, suppressor: OMCOutputSuppressor, expr: str):
    """Run an OMC expression, suppressing stdout/stderr from OMPython."""
    try:
        with suppressor.suppress_omc_output():
            result = omc.sendExpression(expr)
            omc.sendExpression("getErrorString()")
        return result
    except Exception:
        return None


def strip_descriptions_via_omc_ast(
    ast_omc,
    suppressor: OMCOutputSuppressor,
    canonical_file: Path,
    original_class_name: str,
) -> bool:
    """Strip class/component description strings using OMC AST scripting APIs."""
    if _omc_call(ast_omc, suppressor, "clear()") is None:
        return False

    load_expr = f'loadFile("{canonical_file}")'
    loaded = _omc_call(ast_omc, suppressor, load_expr)
    if not loaded:
        return False

    top_classes_raw = _omc_call(ast_omc, suppressor, "getClassNames()")
    if not isinstance(top_classes_raw, (list, tuple)):
        return False

    top_classes = [str(c) for c in top_classes_raw if isinstance(c, str) and c.strip()]
    if not top_classes:
        return False

    all_classes: list[str] = []
    for top in top_classes:
        cls_expr = f"getClassNames({top}, recursive=true, qualified=true)"
        rec = _omc_call(ast_omc, suppressor, cls_expr)
        if isinstance(rec, (list, tuple)):
            all_classes.extend(str(c) for c in rec if isinstance(c, str) and c.strip())

    if not all_classes:
        all_classes = top_classes

    seen: set[str] = set()
    ordered_classes: list[str] = []
    for cls in all_classes:
        if cls not in seen:
            ordered_classes.append(cls)
            seen.add(cls)

    for cls in ordered_classes:
        _omc_call(ast_omc, suppressor, f'OpenModelica.Scripting.setClassComment({cls}, "")')
        _omc_call(ast_omc, suppressor, f'OpenModelica.Scripting.setDocumentationAnnotation({cls}, "", "")')

        comp_count = _omc_call(ast_omc, suppressor, f"OpenModelica.Scripting.getComponentCount({cls})")
        if not isinstance(comp_count, int) or comp_count <= 0:
            continue

        for idx in range(1, comp_count + 1):
            nth = _omc_call(ast_omc, suppressor, f"OpenModelica.Scripting.getNthComponent({cls}, {idx})")
            if not isinstance(nth, (list, tuple)) or len(nth) < 2:
                continue
            component_name = str(nth[1]).strip()
            if not component_name or not component_name.isidentifier():
                continue
            _omc_call(ast_omc, suppressor, f'OpenModelica.Scripting.setComponentComment({cls}, {component_name}, "")')

    short_name = original_class_name.split(".")[-1]
    preferred_total = f"{short_name}_total"
    export_class = next((c for c in top_classes if c.endswith("_total")), "")
    if not export_class and preferred_total in top_classes:
        export_class = preferred_total
    if not export_class:
        export_class = top_classes[-1]

    save_expr = f'saveTotalModel("{canonical_file}", {export_class})'
    saved = _omc_call(ast_omc, suppressor, save_expr)
    return bool(saved) and canonical_file.exists()


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


def read_pilot_sublibraries() -> list[str]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT class_name FROM step3_sublibraries WHERE enabled = 1 ORDER BY class_name"
        ).fetchall()
    return [str(r["class_name"]) for r in rows]


def read_enumerated_classes_for_commit(source_name: str, commit_hash: str, experiments_only: bool = False) -> list[tuple[str, bool]]:
    """Read classes enumerated by Step 2 for a specific commit."""
    with get_step2_classes_connection() as conn:
        if experiments_only:
            rows = conn.execute(
                """
                SELECT class_name, is_experiment
                FROM step2_classes
                WHERE source_name = ? AND commit_hash = ? AND is_experiment = 1
                ORDER BY class_name
                """,
                (source_name, commit_hash),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT class_name, is_experiment
                FROM step2_classes
                WHERE source_name = ? AND commit_hash = ?
                ORDER BY class_name
                """,
                (source_name, commit_hash),
            ).fetchall()
    return [(str(r["class_name"]), bool(r["is_experiment"])) for r in rows]


def read_all_experiment_classes_for_source(source_name: str) -> dict[str, list[tuple[str, bool]]]:
    """Bulk-read all experiment classes for a source, grouped by commit hash.

    Returns a dict mapping commit_hash -> list of (class_name, is_experiment=True).
    One query replaces thousands of per-commit queries against step2_classes.db.
    """
    result: dict[str, list[tuple[str, bool]]] = {}
    with get_step2_classes_connection() as conn:
        rows = conn.execute(
            """
            SELECT commit_hash, class_name
            FROM step2_classes
            WHERE source_name = ? AND is_experiment = 1
            ORDER BY commit_hash, class_name
            """,
            (source_name,),
        ).fetchall()
    for r in rows:
        commit_hash = str(r["commit_hash"])
        result.setdefault(commit_hash, []).append((str(r["class_name"]), True))
    return result


def match_pilot_class(
    class_name: str,
    pilot_items: list[str],
    allow_prefix: bool,
) -> tuple[bool, str, str]:
    if class_name in pilot_items:
        return True, "exact", class_name

    if allow_prefix:
        best = ""
        for item in pilot_items:
            if class_name.startswith(item + ".") and len(item) > len(best):
                best = item
        if best:
            return True, "prefix", best

    return False, "none", ""


def reconcile_step3_db_with_files(cfg: SourceConfig) -> dict[str, int]:
    """Ensure canonical_produced flags reflect actual files on disk."""
    fixed_missing = 0
    fixed_present = 0
    orphan_files = 0
    corrupt_files = 0

    # Safety: if the canonical output base directory is absent in this workspace,
    # do not mutate DB flags/path fields. The DB may have been populated on another
    # machine and the canonical files may simply not be checked out locally.
    if not cfg.canonical_models_dir.exists():
        return {
            "fixed_missing": 0,
            "fixed_present": 0,
            "orphan_files": 0,
            "corrupt_files": 0,
        }

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT commit_hash, class_name, canonical_model_path, canonical_produced
            FROM step3_classes
            WHERE source_name = ?
            """,
            (cfg.name,),
        ).fetchall()

        known_paths: set[str] = set()
        for row in rows:
            canonical_rel = str(row["canonical_model_path"]).strip()
            produced = int(row["canonical_produced"])
            if canonical_rel:
                known_paths.add(canonical_rel)

            # An empty or truncated file must count as absent, otherwise a run
            # interrupted by a full disk gets its damaged output confirmed as
            # canonical here.
            file_exists = bool(canonical_rel) and (PROJECT_ROOT / canonical_rel).exists()
            if file_exists and canonical_output_problem(PROJECT_ROOT / canonical_rel):
                file_exists = False
                corrupt_files += 1

            if produced == 1 and not file_exists:
                conn.execute(
                    """
                    UPDATE step3_classes
                    SET canonical_produced = 0,
                        canonical_model_path = ''
                    WHERE source_name = ? AND commit_hash = ? AND class_name = ?
                    """,
                    (cfg.name, str(row["commit_hash"]), str(row["class_name"])),
                )
                fixed_missing += 1
            elif produced == 0 and file_exists:
                conn.execute(
                    """
                    UPDATE step3_classes
                    SET canonical_produced = 1,
                        canonical_model_path = ?,
                        error_message = ''
                    WHERE source_name = ? AND commit_hash = ? AND class_name = ?
                    """,
                    (canonical_rel, cfg.name, str(row["commit_hash"]), str(row["class_name"])),
                )
                fixed_present += 1

    if cfg.canonical_models_dir.exists():
        disk_paths = {
            str(path.relative_to(PROJECT_ROOT))
            for path in cfg.canonical_models_dir.rglob("*.mo")
            if path.is_file()
        }
        orphan_files = len(disk_paths - known_paths)

    return {
        "fixed_missing": fixed_missing,
        "fixed_present": fixed_present,
        "orphan_files": orphan_files,
        "corrupt_files": corrupt_files,
    }


def upsert_step3_commit_progress(
    source_name: str,
    commit_hash: str,
    status: str,
    processed_classes_count: int,
    saved_models_count: int,
    failure_count: int,
    *,
    db: DBSink | None = None,
) -> None:
    (db or DirectDBSink()).upsert_commit_progress(
        source_name=source_name,
        commit_hash=commit_hash,
        status=status,
        processed_classes_count=processed_classes_count,
        saved_models_count=saved_models_count,
        failure_count=failure_count,
    )


def seed_step3_progress_from_existing_data(source_name: str, commit_hashes: list[str]) -> None:
    commit_set = set(commit_hashes)
    if not commit_set:
        return

    with get_connection() as conn:
        known_progress = {
            str(r["commit_hash"])
            for r in conn.execute(
                "SELECT commit_hash FROM step3_commit_progress WHERE source_name = ?",
                (source_name,),
            ).fetchall()
        }

        class_rows = conn.execute(
            """
            SELECT commit_hash,
                   COUNT(*) AS class_count,
                   SUM(CASE WHEN canonical_produced = 1 THEN 1 ELSE 0 END) AS saved_count
            FROM step3_classes
            WHERE source_name = ?
            GROUP BY commit_hash
            """,
            (source_name,),
        ).fetchall()
        fail_rows = conn.execute(
            """
            SELECT commit_hash, COUNT(*) AS failure_count
            FROM step3_failures
            WHERE source_name = ?
            GROUP BY commit_hash
            """,
            (source_name,),
        ).fetchall()

    class_map = {str(r["commit_hash"]): (int(r["class_count"]), int(r["saved_count"] or 0)) for r in class_rows}
    fail_map = {str(r["commit_hash"]): int(r["failure_count"]) for r in fail_rows}

    for commit_hash in commit_hashes:
        if commit_hash in known_progress:
            continue
        class_count, saved_count = class_map.get(commit_hash, (0, 0))
        failure_count = fail_map.get(commit_hash, 0)
        if class_count > 0 or failure_count > 0:
            upsert_step3_commit_progress(
                source_name,
                commit_hash,
                "complete",
                class_count,
                saved_count,
                failure_count,
            )


def read_step3_progress(source_name: str) -> dict[str, dict[str, int | str]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT commit_hash, status, processed_classes_count, saved_models_count, failure_count
            FROM step3_commit_progress
            WHERE source_name = ?
            """,
            (source_name,),
        ).fetchall()
    return {
        str(r["commit_hash"]): {
            "status": str(r["status"]),
            "processed_classes_count": int(r["processed_classes_count"]),
            "saved_models_count": int(r["saved_models_count"]),
            "failure_count": int(r["failure_count"]),
        }
        for r in rows
    }


def read_step3_commit_scope_metrics(source_name: str, commit_hashes: list[str]) -> dict[str, int]:
    """Count processed commits and commits with canonical outputs for the current Step 3 commit scope."""
    commit_set = set(commit_hashes)
    if not commit_set:
        return {"processed_commits": 0, "saved_commit_folders": 0}

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT commit_hash, status, saved_models_count
            FROM step3_commit_progress
            WHERE source_name = ?
            """,
            (source_name,),
        ).fetchall()

    processed_commits = 0
    saved_commit_folders = 0
    for row in rows:
        commit_hash = str(row["commit_hash"])
        if commit_hash not in commit_set:
            continue
        if str(row["status"]) == "complete":
            processed_commits += 1
            if int(row["saved_models_count"] or 0) > 0:
                saved_commit_folders += 1

    return {
        "processed_commits": processed_commits,
        "saved_commit_folders": saved_commit_folders,
    }


def read_step3_totals(source_name: str, *, pilot_enabled: bool = False) -> dict[str, int]:
    """Read canonical generation stats from step3 and class enumeration stats from step2.

    When *pilot_enabled* is True the step2 database is not queried at all;
    instead the enumeration counts are derived from step3_classes (which in
    pilot mode contains exactly the pilot class rows, all treated as experiments).
    """
    # Canonical generation stats from step3_classes (now only contains experiment candidates)
    with get_connection() as conn:
        classes = conn.execute(
            """
            SELECT COUNT(*) AS total_rows,
                   SUM(CASE WHEN canonical_produced = 1 THEN 1 ELSE 0 END) AS total_saved,
                   COUNT(DISTINCT CASE WHEN canonical_produced = 1 THEN commit_hash END) AS saved_commit_folders,
                   SUM(CASE WHEN is_inside_sublibraries_list = 1 THEN 1 ELSE 0 END) AS whitelisted_rows
            FROM step3_classes
            WHERE source_name = ?
            """,
            (source_name,),
        ).fetchone()
        canonical_fail = conn.execute(
            """
            SELECT COUNT(*) AS canonical_failures
            FROM step3_failures
            WHERE source_name = ? AND failure_type = 'canonical_no_output'
            """,
            (source_name,),
        ).fetchone()
        all_fail = conn.execute(
            "SELECT COUNT(*) AS total_failures FROM step3_failures WHERE source_name = ?",
            (source_name,),
        ).fetchone()

    step3_total = int(classes["total_rows"] or 0)

    if pilot_enabled:
        # In pilot mode every row in step3_classes is an experiment class from
        # the pilot list – no need to query the step2 database at all.
        s2_total_rows = step3_total
        s2_experiment_rows = step3_total
        s2_non_experiment_rows = 0
    else:
        # Class enumeration stats from step2_classes
        with get_step2_classes_connection() as s2conn:
            s2row = s2conn.execute(
                """
                SELECT COUNT(*) AS total_rows,
                       SUM(CASE WHEN is_experiment = 1 THEN 1 ELSE 0 END) AS experiment_rows,
                       SUM(CASE WHEN is_experiment = 0 THEN 1 ELSE 0 END) AS non_experiment_rows
                FROM step2_classes
                WHERE source_name = ?
                """,
                (source_name,),
            ).fetchone()
        s2_total_rows = int(s2row["total_rows"] or 0)
        s2_experiment_rows = int(s2row["experiment_rows"] or 0)
        s2_non_experiment_rows = int(s2row["non_experiment_rows"] or 0)

    return {
        "total_rows": s2_total_rows,
        "experiment_rows": s2_experiment_rows,
        "non_experiment_rows": s2_non_experiment_rows,
        "total_saved": max(
            int(classes["total_saved"] or 0),
            step3_total - int(canonical_fail["canonical_failures"] or 0),
        ),
        "saved_commit_folders": int(classes["saved_commit_folders"] or 0),
        "whitelisted_rows": int(classes["whitelisted_rows"] or 0),
        "canonical_failures": int(canonical_fail["canonical_failures"] or 0),
        "total_failures": int(all_fail["total_failures"] or 0),
    }


def persist_step3_run_values(
    run_id: int,
    total_commits: int,
    processed_commits: int,
    totals: dict[str, int],
    *,
    saved_commit_folders_count: int | None = None,
    enumerated_all_classes: bool,
) -> None:
    total_rows = totals["total_rows"]
    total_saved = totals["total_saved"]
    experiment_rows = totals["experiment_rows"]
    non_experiment_rows = totals["non_experiment_rows"]
    whitelisted_rows = totals["whitelisted_rows"]
    canonical_failures = totals["canonical_failures"]
    saved_commit_folders = (
        int(saved_commit_folders_count)
        if saved_commit_folders_count is not None
        else int(totals.get("saved_commit_folders", 0))
    )

    processed_pct = (100.0 * processed_commits / total_commits) if total_commits else 0.0
    experiment_pct = (100.0 * experiment_rows / total_rows) if total_rows else 0.0
    non_experiment_pct = (100.0 * non_experiment_rows / total_rows) if total_rows else 0.0
    whitelist_pct = (100.0 * whitelisted_rows / experiment_rows) if experiment_rows else 0.0
    extraction_pct = (100.0 * canonical_failures / experiment_rows) if experiment_rows else 0.0

    # Determine the actual input scope for canonical extraction
    if enumerated_all_classes:
        input_scope = experiment_rows
        scope_label = "All experiment classes"
    else:
        input_scope = whitelisted_rows
        scope_label = "Pilot whitelist"

    canonical_saved_pct = (100.0 * total_saved / input_scope) if input_scope else 0.0
    extraction_failures_scope_pct = (100.0 * canonical_failures / input_scope) if input_scope else 0.0

    values: dict[str, int | str] = {
        r"\ProcessedCommitsCount": processed_commits,
        r"\ProcessedCommitsPercentage": f"{processed_pct:.1f}",
        r"\ExtractionFailures": canonical_failures,
        r"\ExtractionFailuresCount": canonical_failures,
        r"\FinalCanonicalModelVersions": total_saved,
        r"\FinalCanonicalModelVersionsPercentage": f"{canonical_saved_pct:.1f}",
        r"\StepOneCommitFoldersWithCanonicals": saved_commit_folders,
        r"\StepThreeInputScope": input_scope,
        r"\StepThreeScopeLabel": scope_label,
        r"\ExtractionFailuresPercentage": f"{extraction_failures_scope_pct:.1f}",
    }

    values.update(
        {
            r"\TotalClassVersions": total_rows,
            r"\ExperimentClassVersions": experiment_rows,
            r"\ExperimentClassVersionsPercentage": f"{experiment_pct:.1f}",
            r"\NonExperimentClassVersions": non_experiment_rows,
            r"\NonExperimentClassVersionsPercentage": f"{non_experiment_pct:.1f}",
            r"\WhitelistedExperimentClassVersions": whitelisted_rows,
            r"\WhitelistedExperimentClassVersionsPercentage": f"{whitelist_pct:.1f}",
        }
    )

    set_run_values(run_id, values)


def prompt_continue_step3(remaining: int, total: int) -> bool:
    if remaining <= 0:
        return False
    while True:
        answer = input(
            f"[PROMPT] Step 3 has {total - remaining:,} processed and {remaining:,} remaining commits. "
            "Continue processing remaining commits? [y/N]: "
        ).strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"", "n", "no"}:
            return False
        print("Please answer 'y' or 'n'.")


def load_modelica_for_commit(omc, suppressor: OMCOutputSuppressor, repo_path: Path, load_targets: list[tuple[str, list[str]]]) -> bool:
    """Load Modelica libraries for the currently checked-out commit.

    Uses explicit ``loadFile`` calls with ``uses=false`` so that OMC does not
    attempt to resolve inter-library dependencies automatically (which can
    fail when the packages live side-by-side in the repo rather than on
    MODELICAPATH).  The load order is determined by the load_targets list
    read from the source's package_file column in step1_sources.

    Each candidate path is resolved in this order: absolute path,
    ``<repo_root>/<candidate>``, then ``<SAM2026_ROOT>/<candidate>``. The
    last fallback lets a source pull in dependencies (e.g. MSL) that live
    under ``source/<other_lib>/`` instead of inside its own worktree.
    """
    try:
        with suppressor.suppress_omc_output():
            omc.sendExpression("clear()")

            repo_root = repo_path.resolve()

            main_pkg_loaded = False
            last_pkg_name = load_targets[-1][0] if load_targets else ""

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
                    # The last target is considered the main/required package
                    if pkg_name == last_pkg_name:
                        return False
                    continue

                load_ok = omc.sendExpression(
                    f'loadFile("{pkg_file}", uses=false)'
                )
                _normalize_omc_error_string(
                    omc.sendExpression("getErrorString()")
                )

                if not bool(load_ok):
                    if pkg_name == last_pkg_name:
                        return False

                if pkg_name == last_pkg_name:
                    main_pkg_loaded = bool(load_ok)

        return main_pkg_loaded
    except Exception:
        return False




# A complete canonical model always ends with the terminating statement of the
# generated total model, e.g. ``end Modelica_Blocks_Examples_Filter_total;``.
_CANONICAL_TERMINATOR_RE = re.compile(rb"^\s*end\s+[A-Za-z_][A-Za-z0-9_.]*\s*;\s*$")


def canonical_output_problem(output_path: Path) -> str:
    """Return a reason string when a saved canonical file is not a complete model.

    ``saveTotalModel`` creates the output file before filling it, so a full disk
    or a crashing OMC session can leave a zero-length or half-written file
    behind. Existence alone therefore does not mean success: without this check
    such a file is recorded as a valid canonical model.
    """
    try:
        size = output_path.stat().st_size
    except OSError as e:
        return f"canonical output could not be stat'ed: {e}"

    if size == 0:
        return "canonical output is empty (0 bytes)"

    try:
        with output_path.open("rb") as fh:
            fh.seek(max(0, size - 4096))
            tail = fh.read()
    except OSError as e:
        return f"canonical output could not be read: {e}"

    lines = [line for line in tail.split(b"\n") if line.strip()]
    if not lines or not _CANONICAL_TERMINATOR_RE.match(lines[-1]):
        return "canonical output is truncated (no terminating 'end <class>;')"

    return ""


def save_canonical_model(
    omc,
    ast_omc,
    suppressor: OMCOutputSuppressor,
    class_name: str,
    output_path: Path,
    strip_descriptions: bool,
) -> tuple[bool, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with suppressor.suppress_omc_output():
            omc.sendExpression(f'saveTotalModel("{output_path}", {class_name})')
            errors = omc.sendExpression("getErrorString()")

        err = _normalize_omc_error_string(errors)

        if output_path.exists():
            if strip_descriptions:
                strip_descriptions_via_omc_ast(ast_omc, suppressor, output_path, class_name)

            # Validate the artifact we are about to record as canonical. A bad
            # file is removed so that a later reconcile pass cannot resurrect it
            # by observing that the path exists.
            problem = canonical_output_problem(output_path)
            if problem:
                with contextlib.suppress(OSError):
                    output_path.unlink()
                return False, f"{problem}; omc reported: {err}" if err else problem

            return True, ""

        return False, err or "saveTotalModel produced no output file"
    except Exception as e:
        return False, str(e)


def record_extraction_failure(
    source_name: str,
    commit_hash: str,
    class_name: str,
    failure_type: str,
    compiler_message: str,
    *,
    db: DBSink | None = None,
) -> None:
    (db or DirectDBSink()).record_failure(
        source_name=source_name,
        commit_hash=commit_hash,
        class_name=class_name,
        failure_type=failure_type,
        compiler_message=compiler_message,
    )


def process_source(cfg: SourceConfig, omc_version: str, start_time: float) -> None:
    print(f"\n{'-' * 50}")
    print(f"  Source: {cfg.name}")
    print(f"{'-' * 50}")

    repo, effective_repo_path = load_repository(cfg.repo_path, cfg.name)
    omc = init_omc_session()
    if omc is None:
        sys.exit(1)

    pilot_enabled = get_setting_bool(_STEP_NUMBER, "PILOT_ENABLED", default=True)
    allow_prefix = get_setting_bool(_STEP_NUMBER, "PILOT_ALLOW_PREFIX", default=True)
    strip_descriptions = get_setting_bool(_STEP_NUMBER, "AST_STRIP_DESCRIPTIONS", default=True)
    # Keep Step 3 bounded by default for expensive runs.
    dry_run_limit = get_setting_int(_STEP_NUMBER, "DRY_RUN_LIMIT")
    pilot_items = read_pilot_sublibraries()
    run_id = start_run_log(
        cfg.name,
        "step3",
        run_settings={
            "pilot_enabled": pilot_enabled,
            "pilot_allow_prefix": allow_prefix,
            "ast_strip_descriptions": strip_descriptions,
            "dry_run_limit": dry_run_limit,
            "pilot_sublibraries_count": len(pilot_items),
            "default_branch": cfg.default_branch,
            "omc_version": omc_version,
        },
    )

    ast_omc = None
    if strip_descriptions:
        ast_omc = init_omc_session()
        if ast_omc is None:
            sys.exit(1)

    commit_hashes = read_commit_hashes_for_source(cfg.name, dry_run_limit)

    reconcile_stats = reconcile_step3_db_with_files(cfg)
    seed_step3_progress_from_existing_data(cfg.name, commit_hashes)
    progress = read_step3_progress(cfg.name)

    processed_commits = {
        commit_hash
        for commit_hash, info in progress.items()
        if info.get("status") == "complete"
    }
    remaining_commits = [c for c in commit_hashes if c not in processed_commits]

    print(f"[INFO]  Commits to process: {len(commit_hashes):,}")
    print(f"[INFO]  Already processed : {len(processed_commits):,}")
    print(f"[INFO]  Remaining         : {len(remaining_commits):,}")
    print(f"[INFO]  Pilot mode enabled : {pilot_enabled}")
    print(f"[INFO]  AST strip enabled  : {strip_descriptions}")
    if pilot_enabled:
        print(f"[INFO]  Pilot classes      : {len(pilot_items):,}")

    print(
        "[INFO]  Reconciliation    : "
        f"fixed_missing={reconcile_stats['fixed_missing']}, "
        f"fixed_present={reconcile_stats['fixed_present']}, "
        f"orphan_files={reconcile_stats['orphan_files']}, "
        f"corrupt_files={reconcile_stats['corrupt_files']}"
    )

    enumerated_all_classes = not pilot_enabled
    totals = read_step3_totals(cfg.name, pilot_enabled=pilot_enabled)
    scoped_metrics = read_step3_commit_scope_metrics(cfg.name, commit_hashes)
    persist_step3_run_values(
        run_id,
        len(commit_hashes),
        scoped_metrics["processed_commits"],
        totals,
        saved_commit_folders_count=scoped_metrics["saved_commit_folders"],
        enumerated_all_classes=enumerated_all_classes,
    )

    if not remaining_commits:
        print("[INFO]  No remaining commits to process.")
    elif not prompt_continue_step3(len(remaining_commits), len(commit_hashes)):
        elapsed = time.time() - start_time
        duration_str = format_duration(elapsed)
        update_run_duration(run_id, duration_str)
        print("[INFO]  Step 3 resume cancelled by user. Existing data kept.")
        try:
            repo.git.checkout(cfg.default_branch, force=True)
        except Exception:
            pass
        return

    # Parallel mode: multiple worker processes with isolated git worktrees,
    # a single DB writer process, and a commit queue.
    requested_workers = get_setting_int(_STEP_NUMBER, "STEP3_WORKERS")
    workers = _auto_worker_count(requested_workers)
    parallel_enabled = workers > 1 and len(remaining_commits) > 1

    interrupted = False
    pbar = None

    # Pre-fetch all experiment classes in one bulk query (non-pilot mode only).
    # This avoids thousands of per-commit queries against step2_classes.db.
    if not pilot_enabled:
        print("[INFO]  Pre-fetching experiment classes from step2 database...")
        experiment_classes_map = read_all_experiment_classes_for_source(cfg.name)
        print(f"[INFO]  Loaded experiment classes for {len(experiment_classes_map):,} commits into memory.")
    else:
        experiment_classes_map = {}

    if parallel_enabled:
        classes_batch_size = get_setting_int(_STEP_NUMBER, "STEP3_DB_FLUSH_CLASSES") or 50
        print("[INFO]  Step 3 execution   : parallel (multi-process commit queue)")
        print(
            f"[INFO]  Parallel workers   : {workers} "
            f"(override with run_settings(step={_STEP_NUMBER},key='STEP3_WORKERS'))"
        )
        print(
            f"[INFO]  SQLite write mode  : single-writer process; class flush batch={classes_batch_size} "
            f"(tune with run_settings(step={_STEP_NUMBER},key='STEP3_DB_FLUSH_CLASSES'))"
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
            pilot_enabled=pilot_enabled,
            allow_prefix=allow_prefix,
            strip_descriptions=strip_descriptions,
            pilot_items=pilot_items,
            workers=workers,
            run_id=run_id,
            commit_hashes=commit_hashes,
            enumerated_all_classes=enumerated_all_classes,
            experiment_classes_map=experiment_classes_map,
        )
        try:
            repo.git.checkout(cfg.default_branch, force=True)
        except Exception:
            pass
        return
    else:
        mode = "sequential" if workers <= 1 else "sequential (parallel disabled: not enough remaining commits)"
        print(f"[INFO]  Step 3 execution   : {mode}")

    try:
        pbar = tqdm(
            remaining_commits,
            total=len(commit_hashes),
            initial=len(processed_commits),
            desc="Processing commits",
            unit="commit",
        )
        db = DirectDBSink()
        suppressor = OMCOutputSuppressor()
        for commit_hash in pbar:
            upsert_step3_commit_progress(cfg.name, commit_hash, "processing", 0, 0, 0, db=db)

            checkout_target = cfg.default_branch if commit_hash == "HEAD" else commit_hash

            try:
                repo.git.checkout(checkout_target, force=True)
            except git.GitCommandError as e:
                msg = f"Failed to checkout commit {checkout_target}: {e}"
                record_extraction_failure(cfg.name, commit_hash, "", "checkout_failed", msg, db=db)
                upsert_step3_commit_progress(cfg.name, commit_hash, "complete", 0, 0, 1, db=db)
                continue

            if not load_modelica_for_commit(omc, suppressor, effective_repo_path, cfg.load_targets):
                msg = "Failed to load Modelica for commit"
                record_extraction_failure(cfg.name, commit_hash, "", "load_model_failed", msg, db=db)
                upsert_step3_commit_progress(cfg.name, commit_hash, "complete", 0, 0, 1, db=db)
                continue

            if pilot_enabled:
                # Use pilot list directly (assume all are experiments)
                classes_with_flags = [(item, True) for item in pilot_items]
            else:
                # Look up from pre-fetched dict (bulk-loaded from step2 DB)
                classes_with_flags = experiment_classes_map.get(commit_hash, [])

            if not classes_with_flags:
                record_extraction_failure(
                    cfg.name,
                    commit_hash,
                    "",
                    "no_classes",
                    "No experiment classes found for this commit.",
                    db=db,
                )
                upsert_step3_commit_progress(cfg.name, commit_hash, "complete", 0, 0, 1, db=db)
                continue

            rows_to_insert: list[tuple] = []
            commit_saved = 0
            commit_failures = 0

            for class_name, is_experiment in classes_with_flags:
                if pilot_enabled:
                    matched, mode, matched_item = match_pilot_class(class_name, pilot_items, allow_prefix)
                    if not matched:
                        continue
                else:
                    matched, mode, matched_item = True, "disabled", ""

                output_file = cfg.canonical_models_dir / commit_hash / f"{class_name.replace('.', '_')}.mo"
                ok, error_message = save_canonical_model(
                    omc,
                    ast_omc,
                    suppressor,
                    class_name,
                    output_file,
                    strip_descriptions,
                )
                canonical_path = str(output_file.relative_to(PROJECT_ROOT)) if ok else ""

                rows_to_insert.append(
                    (
                        cfg.name,
                        commit_hash,
                        class_name,
                        1,
                        canonical_path,
                        error_message,
                        1 if matched else 0,
                        mode,
                        matched_item,
                        1 if ok else 0,
                    )
                )

                if ok:
                    commit_saved += 1
                else:
                    commit_failures += 1
                    record_extraction_failure(
                        cfg.name,
                        commit_hash,
                        class_name,
                        "canonical_no_output",
                        error_message or "saveTotalModel produced no file",
                        db=db,
                    )

            if rows_to_insert:
                db.insert_classes_batch(rows=rows_to_insert)

            upsert_step3_commit_progress(
                cfg.name,
                commit_hash,
                "complete",
                len(rows_to_insert),
                commit_saved,
                commit_failures,
                db=db,
            )
    except KeyboardInterrupt:
        interrupted = True
        print("\n[WARN]  Interrupted by user. Current Step 3 progress has been saved.")

    completed_after = pbar.n if pbar is not None else len(processed_commits)
    totals = read_step3_totals(cfg.name, pilot_enabled=pilot_enabled)
    final_scope_metrics = read_step3_commit_scope_metrics(cfg.name, commit_hashes)
    persist_step3_run_values(
        run_id,
        len(commit_hashes),
        final_scope_metrics["processed_commits"],
        totals,
        saved_commit_folders_count=final_scope_metrics["saved_commit_folders"],
        enumerated_all_classes=enumerated_all_classes,
    )

    generated = utc_now()
    summary_text = (
        f"Step 3 - Runnable Class Extractor Summary\n"
        f"Generated: {generated}\n\n"
        f"OMC version             : {omc_version}\n"
        f"Commits processed       : {completed_after:,} / {len(commit_hashes):,}\n"
        f"Class rows generated    : {totals['total_rows']:,}\n"
        f"Canonical models saved  : {totals['total_saved']:,}\n"
        f"Extraction failures     : {totals['total_failures']:,}\n"
        f"Pilot mode enabled      : {pilot_enabled}\n"
        f"AST strip enabled       : {strip_descriptions}\n"
        f"Pilot sublibrary count  : {len(pilot_items):,}\n"
        f"Interrupted             : {interrupted}\n"
    )

    elapsed = time.time() - start_time
    duration_str = format_duration(elapsed)
    update_run_duration(run_id, duration_str)

    print(summary_text)

    try:
        repo.git.checkout(cfg.default_branch, force=True)
    except Exception:
        pass


def _ensure_clean_worktree_base(repo: git.Repo) -> None:
    try:
        repo.git.worktree("prune")
    except Exception:
        pass


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

    wt_base = SETTINGS_DIR / "worktrees" / "step3" / source_name
    wt_base.mkdir(parents=True, exist_ok=True)
    wt_path = wt_base / f"worker_{worker_id}"

    if wt_path.exists():
        # Best-effort: if worktree is already registered, keep it; otherwise remove dir.
        try:
            # This will fail if it's not a worktree.
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
    Single SQLite writer process.
    This avoids SQLite multi-writer contention and ensures frequent flushes so partial results
    survive interruption.
    """
    sink = DirectDBSink()
    pending_progress: dict[tuple[str, str], dict[str, Any]] = {}

    while True:
        if stop_event.is_set() and op_queue.empty():
            break
        try:
            op = op_queue.get(timeout=0.25)
        except Exception:
            continue

        if op.kind == "failure":
            sink.record_failure(**op.payload)
            continue
        if op.kind == "progress":
            sink.upsert_commit_progress(**op.payload)
            key = (str(op.payload["source_name"]), str(op.payload["commit_hash"]))
            pending_progress[key] = dict(op.payload)
            # Emit a progress event only on completion.
            if str(op.payload.get("status")) == "complete":
                progress_queue.put(ProgressEvent("commit_done", dict(op.payload)))
            continue
        if op.kind == "classes_batch":
            rows = op.payload.get("rows") or []
            if rows:
                sink.insert_classes_batch(rows=rows)
            continue


def _worker_loop(
    *,
    cfg: SourceConfig,
    repo_path: Path,
    default_branch: str,
    commit_queue: "mp.Queue[str]",
    op_queue: "mp.Queue[DBOp]",
    stop_event: "mp.Event",
    pilot_enabled: bool,
    allow_prefix: bool,
    strip_descriptions: bool,
    pilot_items: list[str],
    worker_id: int,
    classes_batch_size: int,
    experiment_classes_map: dict[str, list[tuple[str, bool]]],
) -> None:
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
    ast_omc = init_omc_session() if strip_descriptions else None
    db = QueueDBSink(op_queue)
    suppressor = OMCOutputSuppressor()

    while not stop_event.is_set():
        try:
            commit_hash = commit_queue.get(timeout=0.25)
        except Exception:
            continue

        if not commit_hash:
            break

        db.upsert_commit_progress(
            source_name=cfg.name,
            commit_hash=commit_hash,
            status="processing",
            processed_classes_count=0,
            saved_models_count=0,
            failure_count=0,
        )

        checkout_target = default_branch if commit_hash == "HEAD" else commit_hash

        try:
            repo.git.checkout(checkout_target, force=True)
        except git.GitCommandError as e:
            msg = f"Failed to checkout commit {checkout_target}: {e}"
            record_extraction_failure(cfg.name, commit_hash, "", "checkout_failed", msg, db=db)
            db.upsert_commit_progress(
                source_name=cfg.name,
                commit_hash=commit_hash,
                status="complete",
                processed_classes_count=0,
                saved_models_count=0,
                failure_count=1,
            )
            continue

        if not load_modelica_for_commit(omc, suppressor, wt_path, cfg.load_targets):
            msg = "Failed to load Modelica for commit"
            record_extraction_failure(cfg.name, commit_hash, "", "load_model_failed", msg, db=db)
            db.upsert_commit_progress(
                source_name=cfg.name,
                commit_hash=commit_hash,
                status="complete",
                processed_classes_count=0,
                saved_models_count=0,
                failure_count=1,
            )
            continue

        if pilot_enabled:
            classes_with_flags = [(item, True) for item in pilot_items]
        else:
            # Look up from pre-fetched dict (bulk-loaded from step2 DB)
            classes_with_flags = experiment_classes_map.get(commit_hash, [])

        if not classes_with_flags:
            record_extraction_failure(cfg.name, commit_hash, "", "no_classes", "No experiment classes found for this commit.", db=db)
            db.upsert_commit_progress(
                source_name=cfg.name,
                commit_hash=commit_hash,
                status="complete",
                processed_classes_count=0,
                saved_models_count=0,
                failure_count=1,
            )
            continue

        rows_batch: list[tuple] = []
        processed = 0
        saved = 0
        failures = 0

        for class_name, is_experiment in classes_with_flags:
            if stop_event.is_set():
                break

            if pilot_enabled:
                matched, mode, matched_item = match_pilot_class(class_name, pilot_items, allow_prefix)
                if not matched:
                    continue
            else:
                matched, mode, matched_item = True, "disabled", ""

            output_file = cfg.canonical_models_dir / commit_hash / f"{class_name.replace('.', '_')}.mo"
            ok, error_message = save_canonical_model(
                omc,
                ast_omc,
                suppressor,
                class_name,
                output_file,
                strip_descriptions,
            )
            canonical_path = str(output_file.relative_to(PROJECT_ROOT)) if ok else ""

            rows_batch.append(
                (
                    cfg.name,
                    commit_hash,
                    class_name,
                    1,
                    canonical_path,
                    error_message,
                    1 if matched else 0,
                    mode,
                    matched_item,
                    1 if ok else 0,
                )
            )
            processed += 1
            if ok:
                saved += 1
            else:
                failures += 1
                record_extraction_failure(
                    cfg.name,
                    commit_hash,
                    class_name,
                    "canonical_no_output",
                    error_message or "saveTotalModel produced no file",
                    db=db,
                )

            if len(rows_batch) >= classes_batch_size:
                db.insert_classes_batch(rows=rows_batch)
                rows_batch = []

        # Flush any remaining rows for this commit.
        if rows_batch:
            db.insert_classes_batch(rows=rows_batch)

        db.upsert_commit_progress(
            source_name=cfg.name,
            commit_hash=commit_hash,
            status="complete",
            processed_classes_count=processed,
            saved_models_count=saved,
            failure_count=failures,
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
    pilot_enabled: bool,
    allow_prefix: bool,
    strip_descriptions: bool,
    pilot_items: list[str],
    workers: int,
    run_id: int,
    commit_hashes: list[str],
    enumerated_all_classes: bool,
    experiment_classes_map: dict[str, list[tuple[str, bool]]],
) -> None:
    # Tuning knobs
    classes_batch_size = get_setting_int(_STEP_NUMBER, "STEP3_DB_FLUSH_CLASSES") or 50

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
                "pilot_enabled": pilot_enabled,
                "allow_prefix": allow_prefix,
                "strip_descriptions": strip_descriptions,
                "pilot_items": pilot_items,
                "worker_id": wid,
                "classes_batch_size": classes_batch_size,
                "experiment_classes_map": experiment_classes_map,
            },
            daemon=True,
        )
        p.start()
        procs.append(p)

    pbar = tqdm(
        total=total_commits,
        initial=processed_initial,
        desc="Processing commits",
        unit="commit",
    )

    completed = processed_initial
    try:
        while completed < total_commits:
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

        totals = read_step3_totals(cfg.name, pilot_enabled=pilot_enabled)
        final_scope_metrics = read_step3_commit_scope_metrics(cfg.name, commit_hashes)
        persist_step3_run_values(
            run_id,
            total_commits,
            final_scope_metrics["processed_commits"],
            totals,
            saved_commit_folders_count=final_scope_metrics["saved_commit_folders"],
            enumerated_all_classes=enumerated_all_classes,
        )


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
    sources = get_enabled_sources()

    print("=" * 52)
    print("  Step 3 – Build Canonical Representation")
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

    print(f"\n[INFO]  Step 3 complete for {len(sources)} source(s).")


if __name__ == "__main__":
    main()


