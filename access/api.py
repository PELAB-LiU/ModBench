"""Read-only API for the ModBench (SAM 2026) dataset.

Wraps the three persistent stores produced by the generation pipeline
(see paper Section 4):

  * ``dataset/pipeline.db``      -- step1_* and step3_* tables
  * ``dataset/step2_classes.db`` -- the class-listing store
  * ``dataset/canonical_models`` -- canonical ``.mo`` files on disk

The API supports:

  * listing source libraries, commits, and experiment classes;
  * retrieving all canonicalised class versions in commit order;
  * listing all models associated with a commit;
  * reading canonical source for a specific class at a specific revision;
  * inspecting recorded canonicalisation failures;
  * resolving GitHub commit / pull-request / issue metadata.

Downstream projects (e.g. SoSym2026) build on this API by composition;
this module knows nothing about them.

Usage::

    from access.api import ModelicaDataset

    with ModelicaDataset() as ds:
        for snap in ds.get_class_timeline("MSL", "Modelica.Blocks.Examples.Filter"):
            print(snap.commit_hash, snap.canonical_model_path)
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Path / env setup (no dependency on the pipeline package).
# ---------------------------------------------------------------------------

_THIS_DIR = Path(__file__).resolve().parent
_SAM_ROOT = _THIS_DIR.parent

# Optionally load .env so CANONICAL_REMOTE_HOST / CANONICAL_REMOTE_BASE
# are available for the SSH fallback in read_canonical_model().
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(_SAM_ROOT / ".env")
except ImportError:
    pass

DB_PATH = _SAM_ROOT / "dataset" / "pipeline.db"
STEP2_CLASSES_DB_PATH = _SAM_ROOT / "dataset" / "step2_classes.db"
CANONICAL_MODELS_BASE_DIR = _SAM_ROOT / "dataset" / "canonical_models"


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClassVersion:
    """A single (commit, class) canonical snapshot from Step 3."""

    commit_hash: str
    class_name: str
    canonical_model_path: str
    is_experiment: bool


@dataclass(frozen=True)
class CommitInfo:
    """Lightweight commit metadata from ``step1_commits``."""

    commit_hash: str
    author_email: str
    commit_message: str
    excluded: bool
    exclusion_reason: str


@dataclass(frozen=True)
class CanonicalizationFailure:
    """A recorded canonicalisation failure from ``step3_failures``."""

    commit_hash: str
    class_name: str
    failure_type: str
    compiler_message: str
    created_at_utc: str


@dataclass(frozen=True)
class GitHubCommitInfo:
    sha: str
    url: str
    author_login: str
    author_name: str
    author_email: str
    authored_date: str
    committer_login: str
    committer_name: str
    committed_date: str
    message: str
    files_changed: int
    additions: int
    deletions: int
    changed_files: list[str]


@dataclass(frozen=True)
class GitHubPullRequest:
    number: int
    title: str
    url: str
    state: str
    merged: bool
    labels: list[str]


@dataclass(frozen=True)
class GitHubIssue:
    number: int
    title: str
    url: str
    state: str
    labels: list[str]


_GITHUB_REPOS: dict[str, str] = {
    "MSL": "modelica/ModelicaStandardLibrary",
    "Buildings": "lbl-srg/modelica-buildings",
    "OpenIPSL": "OpenIPSL/OpenIPSL",
    "ScalableTestSuite": "casella/ScalableTestSuite",
    "ScalableTestGrids": "modelica/ScalableTestGrids",
    "ThermofluidStream": "DLR-SR/ThermofluidStream",
}


# ---------------------------------------------------------------------------
# Main API class
# ---------------------------------------------------------------------------

class ModelicaDataset:
    """Read-only gateway to the ModBench dataset (steps 1--3 stores)."""

    def __init__(
        self,
        db_path: Path | str | None = None,
        step2_db_path: Path | str | None = None,
    ) -> None:
        self._db_path = Path(db_path or DB_PATH)
        self._step2_db_path = Path(step2_db_path or STEP2_CLASSES_DB_PATH)
        self._conn: sqlite3.Connection | None = None
        self._step2_conn: sqlite3.Connection | None = None

    # ---- context manager --------------------------------------------------

    def __enter__(self) -> "ModelicaDataset":
        self.open()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def open(self) -> None:
        if self._conn is None:
            self._conn = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
            self._conn.row_factory = sqlite3.Row
        if self._step2_conn is None and self._step2_db_path.exists():
            self._step2_conn = sqlite3.connect(
                f"file:{self._step2_db_path}?mode=ro", uri=True
            )
            self._step2_conn.row_factory = sqlite3.Row

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        if self._step2_conn is not None:
            self._step2_conn.close()
            self._step2_conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self.open()
        assert self._conn is not None
        return self._conn

    @property
    def step2_conn(self) -> sqlite3.Connection | None:
        if self._step2_conn is None and self._step2_db_path.exists():
            self.open()
        return self._step2_conn

    # ---- sources & commits ------------------------------------------------

    def list_sources(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT source_name FROM step1_commits ORDER BY source_name"
        ).fetchall()
        return [str(r["source_name"]) for r in rows]

    def list_commits(
        self, source_name: str, *, include_excluded: bool = False
    ) -> list[str]:
        sql = "SELECT commit_hash FROM step1_commits WHERE source_name = ?"
        if not include_excluded:
            sql += " AND excluded = 0"
        sql += " ORDER BY rowid"
        return [str(r["commit_hash"]) for r in self.conn.execute(sql, (source_name,)).fetchall()]

    def get_commit(self, source_name: str, commit_hash: str) -> CommitInfo | None:
        row = self.conn.execute(
            """
            SELECT commit_hash, author_email, commit_message, excluded, exclusion_reason
            FROM step1_commits WHERE source_name = ? AND commit_hash = ?
            """,
            (source_name, commit_hash),
        ).fetchone()
        if row is None:
            return None
        return CommitInfo(
            commit_hash=str(row["commit_hash"]),
            author_email=str(row["author_email"] or ""),
            commit_message=str(row["commit_message"] or ""),
            excluded=bool(row["excluded"]),
            exclusion_reason=str(row["exclusion_reason"] or ""),
        )

    # ---- experiment classes / class listing ------------------------------

    def list_experiment_classes(self, source_name: str) -> list[str]:
        """Distinct experiment class names that have at least one
        canonicalised snapshot in Step 3."""
        rows = self.conn.execute(
            """
            SELECT DISTINCT class_name FROM step3_classes
            WHERE source_name = ?
              AND is_experiment = 1
              AND canonical_produced = 1
            ORDER BY class_name
            """,
            (source_name,),
        ).fetchall()
        return [str(r["class_name"]) for r in rows]

    def get_enumerated_classes(
        self, source_name: str, commit_hash: str
    ) -> list[tuple[str, bool]]:
        """``(class_name, is_experiment)`` pairs from the Step 2 listing
        for a given commit."""
        if self.step2_conn is None:
            return []
        rows = self.step2_conn.execute(
            """
            SELECT class_name, is_experiment FROM step2_classes
            WHERE source_name = ? AND commit_hash = ? ORDER BY class_name
            """,
            (source_name, commit_hash),
        ).fetchall()
        return [(str(r["class_name"]), bool(r["is_experiment"])) for r in rows]

    # ---- class timeline ---------------------------------------------------

    def get_class_timeline(
        self, source_name: str, class_name: str
    ) -> list[ClassVersion]:
        """Every canonicalised snapshot for a class, in commit order."""
        rows = self.conn.execute(
            """
            SELECT sc.commit_hash,
                   sc.class_name,
                   COALESCE(sc.canonical_model_path, '') AS canonical_model_path,
                   sc.is_experiment
            FROM step3_classes sc
            JOIN step1_commits c
              ON c.source_name = sc.source_name AND c.commit_hash = sc.commit_hash
            WHERE sc.source_name = ?
              AND sc.class_name = ?
              AND sc.canonical_produced = 1
            ORDER BY c.rowid
            """,
            (source_name, class_name),
        ).fetchall()
        return [
            ClassVersion(
                commit_hash=str(r["commit_hash"]),
                class_name=str(r["class_name"]),
                canonical_model_path=str(r["canonical_model_path"]),
                is_experiment=bool(r["is_experiment"]),
            )
            for r in rows
        ]

    def get_models_for_commit(
        self, source_name: str, commit_hash: str
    ) -> list[ClassVersion]:
        """All experiment-class canonical snapshots produced for a commit."""
        rows = self.conn.execute(
            """
            SELECT class_name,
                   COALESCE(canonical_model_path, '') AS canonical_model_path,
                   is_experiment
            FROM step3_classes
            WHERE source_name = ? AND commit_hash = ? AND canonical_produced = 1
            ORDER BY class_name
            """,
            (source_name, commit_hash),
        ).fetchall()
        return [
            ClassVersion(
                commit_hash=commit_hash,
                class_name=str(r["class_name"]),
                canonical_model_path=str(r["canonical_model_path"]),
                is_experiment=bool(r["is_experiment"]),
            )
            for r in rows
        ]

    # ---- canonicalisation failures ---------------------------------------

    def list_canonicalization_failures(
        self,
        source_name: str,
        *,
        class_name: str | None = None,
    ) -> list[CanonicalizationFailure]:
        """Recorded ``step3_failures`` entries for a source, optionally
        filtered by class name."""
        sql = (
            "SELECT commit_hash, class_name, failure_type, compiler_message, created_at_utc "
            "FROM step3_failures WHERE source_name = ?"
        )
        params: list[Any] = [source_name]
        if class_name is not None:
            sql += " AND class_name = ?"
            params.append(class_name)
        sql += " ORDER BY id"
        return [
            CanonicalizationFailure(
                commit_hash=str(r["commit_hash"]),
                class_name=str(r["class_name"]),
                failure_type=str(r["failure_type"] or ""),
                compiler_message=str(r["compiler_message"] or ""),
                created_at_utc=str(r["created_at_utc"] or ""),
            )
            for r in self.conn.execute(sql, params).fetchall()
        ]

    # ---- canonical file content ------------------------------------------

    def read_canonical_model(self, canonical_model_path: str) -> str | None:
        """Read a canonical ``.mo`` file.

        Resolution order: (1) local cache under ``CANONICAL_MODELS_BASE_DIR``,
        (2) remote artifact server via SSH, (3) regenerate via OMC.
        """
        rel = _strip_to_canonical_relative(canonical_model_path)
        full = CANONICAL_MODELS_BASE_DIR / rel
        if full.is_file():
            return full.read_text(encoding="utf-8", errors="replace")

        # 2) Remote fetch.
        raw = _read_canonical_bytes_remote(canonical_model_path)
        if raw is not None:
            return raw.decode("utf-8", errors="replace")

        # 3) OMC regenerate fallback.
        regenerated = _regenerate_canonical(canonical_model_path)
        if regenerated and regenerated.is_file():
            return regenerated.read_text(encoding="utf-8", errors="replace")
        return None

    # ---- GitHub helpers ---------------------------------------------------

    @staticmethod
    def _github_repo_for_source(source_name: str) -> str:
        repo = _GITHUB_REPOS.get(source_name)
        if not repo:
            raise ValueError(
                f"No GitHub repository mapping for source '{source_name}'. "
                f"Known sources: {', '.join(sorted(_GITHUB_REPOS))}"
            )
        return repo

    @staticmethod
    def _github_get(url: str) -> Any:
        headers = {"Accept": "application/vnd.github+json"}
        token = os.environ.get("GITHUB_TOKEN", "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(f"GitHub API error {exc.code} for {url}: {body}") from exc

    def get_github_commit(
        self, source_name: str, commit_hash: str
    ) -> GitHubCommitInfo:
        repo = self._github_repo_for_source(source_name)
        data = self._github_get(f"https://api.github.com/repos/{repo}/commits/{commit_hash}")
        author = data.get("author") or {}
        commit = data.get("commit") or {}
        c_author = commit.get("author") or {}
        c_committer = commit.get("committer") or {}
        stats = data.get("stats") or {}
        files = data.get("files") or []
        return GitHubCommitInfo(
            sha=str(data.get("sha", commit_hash)),
            url=str(data.get("html_url", "")),
            author_login=str(author.get("login", "")),
            author_name=str(c_author.get("name", "")),
            author_email=str(c_author.get("email", "")),
            authored_date=str(c_author.get("date", "")),
            committer_login=str((data.get("committer") or {}).get("login", "")),
            committer_name=str(c_committer.get("name", "")),
            committed_date=str(c_committer.get("date", "")),
            message=str(commit.get("message", "")),
            files_changed=int(stats.get("total", len(files))),
            additions=int(stats.get("additions", 0)),
            deletions=int(stats.get("deletions", 0)),
            changed_files=[str(f.get("filename", "")) for f in files],
        )

    def get_github_pull_requests(
        self, source_name: str, commit_hash: str
    ) -> list[GitHubPullRequest]:
        repo = self._github_repo_for_source(source_name)
        data = self._github_get(
            f"https://api.github.com/repos/{repo}/commits/{commit_hash}/pulls"
        )
        if not isinstance(data, list):
            return []
        return [
            GitHubPullRequest(
                number=int(pr.get("number", 0)),
                title=str(pr.get("title", "")),
                url=str(pr.get("html_url", "")),
                state=str(pr.get("state", "")),
                merged=bool(pr.get("merged_at")),
                labels=[str(lb.get("name", "")) for lb in (pr.get("labels") or [])],
            )
            for pr in data
        ]

    def get_github_issue(self, source_name: str, issue_number: int) -> GitHubIssue:
        repo = self._github_repo_for_source(source_name)
        data = self._github_get(f"https://api.github.com/repos/{repo}/issues/{issue_number}")
        return GitHubIssue(
            number=int(data.get("number", issue_number)),
            title=str(data.get("title", "")),
            url=str(data.get("html_url", "")),
            state=str(data.get("state", "")),
            labels=[str(lb.get("name", "")) for lb in (data.get("labels") or [])],
        )

    def get_github_linked_issues(
        self, source_name: str, commit_hash: str
    ) -> list[GitHubIssue]:
        info = self.get_commit(source_name, commit_hash)
        if not info or not info.commit_message:
            return []
        numbers = sorted({int(m) for m in re.findall(r"#(\d+)", info.commit_message)})
        issues: list[GitHubIssue] = []
        for num in numbers:
            try:
                issues.append(self.get_github_issue(source_name, num))
            except RuntimeError:
                pass
        return issues

    # ---- summary ---------------------------------------------------------

    def summary(self, source_name: str) -> dict[str, int]:
        """High-level counts (steps 1--3) for a source."""
        c = self.conn
        total_commits = c.execute(
            "SELECT COUNT(*) FROM step1_commits WHERE source_name = ? AND excluded = 0",
            (source_name,),
        ).fetchone()[0]
        canonical_snapshots = c.execute(
            "SELECT COUNT(*) FROM step3_classes "
            "WHERE source_name = ? AND canonical_produced = 1",
            (source_name,),
        ).fetchone()[0]
        experiment_classes = c.execute(
            "SELECT COUNT(DISTINCT class_name) FROM step3_classes "
            "WHERE source_name = ? AND is_experiment = 1 AND canonical_produced = 1",
            (source_name,),
        ).fetchone()[0]
        failures = c.execute(
            "SELECT COUNT(*) FROM step3_failures WHERE source_name = ?",
            (source_name,),
        ).fetchone()[0]
        return {
            "total_commits": int(total_commits),
            "canonical_snapshots": int(canonical_snapshots),
            "experiment_classes": int(experiment_classes),
            "canonicalization_failures": int(failures),
        }


# ---------------------------------------------------------------------------
# Helpers shared with downstream tools
# ---------------------------------------------------------------------------

def _strip_to_canonical_relative(stored_path: str) -> Path:
    """Map a stored ``canonical_model_path`` to a path relative to
    ``CANONICAL_MODELS_BASE_DIR``.

    Accepts both the legacy
    ``dataset_creator/canonical_models/...`` form and the new
    ``<source>/<commit>/<cls>.mo`` form.
    """
    p = Path(stored_path)
    parts = p.parts
    if "canonical_models" in parts:
        idx = parts.index("canonical_models")
        return Path(*parts[idx + 1 :])
    return p


# ---------------------------------------------------------------------------
# Remote fallback: fetch a canonical .mo file over SSH
# ---------------------------------------------------------------------------

def _read_canonical_bytes_remote(canonical_model_path: str) -> bytes | None:
    """Fetch raw bytes via ``ssh $CANONICAL_REMOTE_HOST cat ...`` when
    configured; return ``None`` if no remote is set or the fetch fails."""
    rel = (canonical_model_path or "").strip()
    if not rel:
        return None
    remote_host = os.environ.get("CANONICAL_REMOTE_HOST", "").strip()
    remote_base = os.environ.get("CANONICAL_REMOTE_BASE", "").strip()
    if not remote_host or not remote_base:
        return None
    try:
        result = subprocess.run(
            ["ssh", remote_host, "cat", f"{remote_base}/{rel}"],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 0:
            return result.stdout
    except (subprocess.TimeoutExpired, OSError):
        pass
    return None


# ---------------------------------------------------------------------------
# OMC fallback: regenerate a canonical .mo file on demand
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _SourceConfig:
    """Minimal source-config view used by the OMC regenerate fallback."""

    name: str
    repo_path: Path
    package_file: str

    @property
    def load_targets(self) -> list[tuple[str, list[str]]]:
        """Parse ``package_file`` into ``[(pkg_name, [candidate_paths])]``.

        Format: ``PkgA=path1,path2;PkgB=path3``. Legacy single-path entries
        (no ``=``) are also accepted.
        """
        targets: list[tuple[str, list[str]]] = []
        for entry in self.package_file.split(";"):
            entry = entry.strip()
            if not entry:
                continue
            if "=" in entry:
                pkg_name, paths_str = entry.split("=", 1)
                candidates = [p.strip() for p in paths_str.split(",") if p.strip()]
            else:
                candidates = [entry]
                pkg_name = entry.split("/")[0]
            targets.append((pkg_name.strip(), candidates))
        return targets


def _resolve_repo_path(raw: str, source_name: str) -> Path:
    p = Path(raw)
    if not p.is_absolute():
        return _SAM_ROOT / p
    if p.exists():
        return p
    preferred = _SAM_ROOT / "source" / source_name
    if preferred.exists():
        return preferred
    if "source" in p.parts:
        idx = p.parts.index("source")
        return _SAM_ROOT / Path(*p.parts[idx:])
    return p


def _get_source_config(source_name: str) -> _SourceConfig | None:
    if not DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT name, repo_path, package_file FROM step1_sources "
            "WHERE name = ? AND enabled = 1",
            (source_name,),
        ).fetchone()
        conn.close()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    return _SourceConfig(
        name=str(row["name"]),
        repo_path=_resolve_repo_path(str(row["repo_path"]), str(row["name"])),
        package_file=str(row["package_file"]),
    )


def _parse_canonical_path(canonical_model_path: str) -> tuple[str, str, str] | None:
    """Extract ``(source_name, commit_hash, class_name)`` from a stored path."""
    parts = Path(canonical_model_path).parts
    if "canonical_models" in parts:
        parts = parts[parts.index("canonical_models") + 1 :]
    if len(parts) < 3:
        return None
    return parts[0], parts[1], Path(parts[-1]).stem


def _omc_load_and_save(cfg: _SourceConfig, repo_root: Path, class_name: str, out_path: Path) -> bool:
    try:
        from OMPython import OMCSessionZMQ  # type: ignore
    except ImportError:
        return False

    omc = OMCSessionZMQ()
    try:
        omc.sendExpression("clear()")
        last_pkg = cfg.load_targets[-1][0] if cfg.load_targets else ""
        for pkg_name, candidates in cfg.load_targets:
            pkg_file: Path | None = None
            for c in candidates:
                for base in (repo_root, _SAM_ROOT):
                    p = base / c
                    if p.is_file():
                        pkg_file = p
                        break
                if pkg_file is not None:
                    break
            if pkg_file is None:
                if pkg_name == last_pkg:
                    return False
                continue
            ok = omc.sendExpression(f'loadFile("{pkg_file}", uses=false)')
            omc.sendExpression("getErrorString()")
            if not ok and pkg_name == last_pkg:
                return False

        out_path.parent.mkdir(parents=True, exist_ok=True)
        omc.sendExpression(f'saveTotalModel("{out_path}", {class_name})')
        omc.sendExpression("getErrorString()")
        return out_path.exists()
    finally:
        try:
            omc.sendExpression("quit()")
        except Exception:
            pass


def _regenerate_canonical(canonical_model_path: str) -> Path | None:
    """Check out the source commit in a temp worktree and run OMC to
    regenerate ``canonical_model_path``; return the cached path on success."""
    parsed = _parse_canonical_path(canonical_model_path)
    if parsed is None:
        return None
    source_name, commit_hash, class_name = parsed

    cfg = _get_source_config(source_name)
    if cfg is None:
        return None

    out_path = CANONICAL_MODELS_BASE_DIR / source_name / commit_hash / f"{class_name}.mo"
    if out_path.is_file():
        return out_path

    try:
        import git  # type: ignore
        repo = git.Repo(cfg.repo_path)
    except Exception:
        return None

    base = _SAM_ROOT / "worktrees" / "regen"
    base.mkdir(parents=True, exist_ok=True)
    wt_path = Path(tempfile.mkdtemp(prefix=f"{commit_hash[:8]}_", dir=base))
    try:
        repo.git.worktree("add", "--detach", str(wt_path), commit_hash)
        if _omc_load_and_save(cfg, wt_path, class_name, out_path):
            return out_path
    except Exception:
        return None
    finally:
        try:
            repo.git.worktree("remove", "--force", str(wt_path))
        except Exception:
            shutil.rmtree(wt_path, ignore_errors=True)
    return None

