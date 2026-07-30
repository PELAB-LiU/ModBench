"""Shared configuration and SQLite helpers for the ModBench pipeline.

Steps 1--3 produce:
  Step 1 -> step1_sources, step1_commits, step1_files
  Step 2 -> step2_classes (separate DB)
  Step 3 -> step3_classes, step3_failures + canonical .mo files
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# DIRECTORY LAYOUT
# ---------------------------------------------------------------------------

SAM2026_ROOT = Path(__file__).resolve().parent.parent

# Load environment variables from a local .env file (if present) so that
# SSH hosts, remote paths and similar deployment-specific values stay
# out of the published source tree. See ``.env.example``.
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(SAM2026_ROOT / ".env")
except ImportError:
    pass

SOURCE_DIR = SAM2026_ROOT / "source"
DATASET_DIR = SAM2026_ROOT / "dataset"
CANONICAL_MODELS_BASE_DIR = DATASET_DIR / "canonical_models"
DB_PATH = DATASET_DIR / "pipeline.db"
STEP2_CLASSES_DB_PATH = DATASET_DIR / "step2_classes.db"

# Generated reporting artifacts. Everything the report script produces lives
# in this directory inside the repository; consumers copy from here.
RESULTS_DIR = SAM2026_ROOT / "results"
VARIABLES_TEX_PATH = RESULTS_DIR / "variables.tex"

DATASET_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# SOURCE CONFIGURATION
# ---------------------------------------------------------------------------

@dataclass
class SourceConfig:
    """Runtime source configuration loaded from the SQLite registry."""

    name: str
    repo_path: Path
    package_file: str
    enabled: bool
    excluded_branches: list[str]
    default_branch: str = "master"
    commit_cutoff_tag: str | None = None

    @property
    def canonical_models_dir(self) -> Path:
        return CANONICAL_MODELS_BASE_DIR / self.name

    @property
    def load_targets(self) -> list[tuple[str, list[str]]]:
        """Parse ``package_file`` into ``[(package_name, [candidate_paths])]``.

        Format: ``PkgA=path1,path2;PkgB=path3``.  Legacy single-path form
        (no ``=``) is also accepted.
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


# ---------------------------------------------------------------------------
# SEEDED DEFAULTS
# ---------------------------------------------------------------------------

DEFAULT_SOURCES: list[tuple[str, str, str, int, str, str, str | None]] = [
    # ModelicaServices moved during MSL history: older revisions ship it as
    # tool-specific variants under ModelicaServices-Variants/, newer ones as a
    # plain top-level ModelicaServices/. Both paths are listed so the library's
    # own implementation is always loaded; "Default" is the variant the MSL
    # readme describes as working for every tool. Without the variant path,
    # every model reaching ModelicaServices.Animation.Shape (i.e. the MultiBody
    # visualizers) fails with an unresolvable base class on those revisions.
    ("MSL", "source/MSL",
     "ModelicaServices=ModelicaServices/package.mo,ModelicaServices-Variants/Default/ModelicaServices/package.mo;Complex=Complex.mo,Complex/package.mo;Modelica=Modelica/package.mo",
     1, "maint/2.2.2;maint/2.2.1;gh-pages", "master", "v3.0"),
    ("Buildings", "source/Buildings", "Buildings/package.mo", 0, "", "master", None),
    ("OpenIPSL", "source/OpenIPSL", "OpenIPSL/package.mo", 0, "", "master", None),
    ("ScalableTestSuite", "source/ScalableTestSuite", "ScalableTestSuite/package.mo", 0, "", "master", None),
    ("ScalableTestGrids", "source/ScalableTestGrids", "ScalableTestGrids/package.mo", 0, "", "master", None),
    ("FirstBookExamples", "source/FirstBookExamples", "FirstBookExamples/package.mo", 0, "", "master", None),
    ("modelicaExamples", "source/modelicaExamples", "modelicaExamples/package.mo", 0, "", "main", None),
    ("ThermofluidStream", "source/ThermofluidStream",
     "Modelica=source/MSL/Modelica/package.mo;"
     "Complex=source/MSL/Complex.mo,source/MSL/Complex/package.mo;"
     "ThermofluidStream=ThermofluidStream/package.mo",
     1, "", "main", None),
]

DEFAULT_PILOT_SUBLIBRARIES: list[str] = [
    "Modelica.Blocks.Examples.BooleanNetwork1",
    "Modelica.Blocks.Examples.Filter",
    "Modelica.Clocked.Examples.CascadeControlledDrive.AbsoluteClocks",
    "Modelica.Clocked.Examples.Elementary.ClockSignals.SubSample",
    "Modelica.Clocked.Examples.Systems.EngineThrottleControl",
    "Modelica.ComplexBlocks.Examples.TestConversionBlock",
    "Modelica.Electrical.Analog.Examples.ChuaCircuit",
    "Modelica.Electrical.Batteries.Examples.CCCVcharging",
    "Modelica.Electrical.Digital.Examples.Adder4",
    "Modelica.Electrical.Machines.Examples.DCMachines.DCPM_QuasiStatic",
    "Modelica.Electrical.Machines.Examples.InductionMachines.IMC_Transformer",
    "Modelica.Electrical.Machines.Examples.SynchronousMachines.SMEE_Rectifier",
    "Modelica.Electrical.Machines.Examples.Transformers.TransformerTestbench",
    "Modelica.Electrical.Polyphase.Examples.TransformerYD",
    "Modelica.Electrical.PowerConverters.Examples.DCDC.HBridge.HBridge_DC_Drive",
    "Modelica.Electrical.QuasiStatic.Machines.Examples.TransformerTestbench",
    "Modelica.Electrical.QuasiStatic.Polyphase.Examples.TestSensors",
    "Modelica.Electrical.QuasiStatic.SinglePhase.Examples.Rectifier",
    "Modelica.Electrical.QuasiStatic.SinglePhase.Examples.Transformer",
    "Modelica.Electrical.Spice3.Examples.Spice3BenchmarkFourBitBinaryAdder",
    "Modelica.Fluid.Examples.AST_BatchPlant.Test.TwoTanks",
    "Modelica.Fluid.Examples.BranchingDynamicPipes",
    "Modelica.Magnetic.FluxTubes.Examples.BasicExamples.SaturatedInductor",
    "Modelica.Magnetic.FluxTubes.Examples.Hysteresis.HysteresisModelComparison",
    "Modelica.Magnetic.QuasiStatic.FluxTubes.Examples.BasicExamples.QuadraticCoreAirgap",
    "Modelica.Magnetic.QuasiStatic.FundamentalWave.Examples.BasicMachines.InductionMachines.IMC_Conveyor",
    "Modelica.Magnetic.FundamentalWave.Examples.BasicMachines.InductionMachines.ComparisonPolyphase.IMC_DOL_Polyphase",
    "Modelica.Math.FastFourierTransform.Examples.RealFFT1",
    "Modelica.Math.Random.Examples.GenerateRandomNumbers",
    "Modelica.Mechanics.MultiBody.Examples.Loops.EngineV6",
    "Modelica.Mechanics.Rotational.Examples.RollingWheel",
    "Modelica.Mechanics.Translational.Examples.Vehicle",
    "Modelica.Media.Examples.MoistAir",
    "Modelica.Media.Examples.SimpleLiquidWater",
    "Modelica.Media.Examples.WaterIF97",
    "Modelica.Media.Incompressible.Examples.TestGlycol",
    "Modelica.Thermal.FluidHeatFlow.Examples.TestCylinder",
    "Modelica.Thermal.FluidHeatFlow.Examples.TestOpenTank",
    "Modelica.Thermal.HeatTransfer.Examples.Motor",
    "Modelica.Utilities.Examples.WriteRealMatrixToFile",
]

# Step-local runtime settings, persisted in DB on first init.
DEFAULT_PIPELINE_SETTINGS: list[tuple[int, str, str | None]] = [
    (1, "COMMIT_AGE_LIMIT_YEARS", ""),
    (2, "DRY_RUN_LIMIT", None),
    (3, "DRY_RUN_LIMIT", None),
    (3, "PILOT_ENABLED", "1"),
    (3, "PILOT_ALLOW_PREFIX", "1"),
]

# Keys no step reads, as (step, key). _remove_obsolete_run_settings() deletes
# them from a database that still carries them, so nobody sets a value that has
# no effect. Removing non-semantic annotations, for instance, is not
# configurable: it costs a fraction of a percent of a commit, so step 3 always
# does it.
REMOVED_PIPELINE_SETTINGS: list[tuple[int, str]] = [
    (3, "AST_STRIP_DESCRIPTIONS"),
    (3, "STRIP_DESCRIPTIONS"),
    (3, "STRIP_NON_SEMANTIC_ANNOTATIONS"),
]


# ---------------------------------------------------------------------------
# BOT-DETECTION HEURISTICS (used by step 1)
# ---------------------------------------------------------------------------

BOT_NAME_PATTERNS: list[str] = []
BOT_EMAIL_PATTERNS: list[str] = []
BOT_MESSAGE_HUMAN_OVERRIDE_STRINGS: list[str] = [r"^Fix\b"]
BOT_DOC_ONLY_PATTERN_STRINGS: list[str] = [r"\bUpdate\s+release\s+notes\b"]
BOT_DOC_EXTENSIONS: frozenset[str] = frozenset(
    {".html", ".htm", ".pdf", ".md", ".rst", ".mo", ".txt", ".csv"}
)
BOT_MESSAGE_PATTERN_STRINGS: list[str] = [
    r"\[(?:skip\s*[-_ ]?\s*ci|ci\s*[-_ ]?\s*skip)\]",
    r"\bUpdate\s+versionDate\b",
    r"\bVersion\s+bump\b",
    r"\b(Bump|Auto-?update)\s+version\b",
    r"^Update\s+to\s+v\d",
    r"\bUpdate\s+version\b",
    r"\bUpdate\s+\.mailmap\b",
    r"\bUpdate\s+release\s+notes\b",
]


# ---------------------------------------------------------------------------
# REPORT METRIC -> LaTeX MACRO MAPPINGS (steps 1--3 only)
# ---------------------------------------------------------------------------


LATEX_TO_PIPELINE_STEP: dict[str, int] = {
    r"\StepOneInputUniqueCommits": 1,
    r"\StepOneExcludedBranchesCount": 1,
    r"\StepOneExcludedBranchesPct": 1,
    r"\StepOneExcludedPreVThreeCount": 1,
    r"\StepOneExcludedPreVThreePct": 1,
    r"\StepOneExcludedBotCount": 1,
    r"\StepOneExcludedBotPct": 1,
    r"\StepOneExcludedNoMoCount": 1,
    r"\StepOneExcludedNoMoPct": 1,
    r"\StepOneCommitMoPairs": 1,
    r"\StepOneCommitMoPairsMean": 1,
    r"\StepOneCommitMoPairsMedian": 1,
    r"\StepOneTotalExcludedCount": 1,
    r"\StepOneTotalExcludedPct": 1,
    r"\TotalCommitsFiltered": 1,
    r"\ProcessedCommitsCount": 3,
    r"\ProcessedCommitsPercentage": 3,
    r"\TotalClassVersions": 3,
    r"\ExperimentClassVersions": 3,
    r"\ExperimentClassVersionsPercentage": 3,
    r"\NonExperimentClassVersions": 3,
    r"\NonExperimentClassVersionsPercentage": 3,
    r"\WhitelistedExperimentClassVersions": 3,
    r"\WhitelistedExperimentClassVersionsPercentage": 3,
    r"\StepTwoClassesMean": 2,
    r"\StepTwoClassesMedian": 2,
    r"\StepTwoClassesMax": 2,
    r"\StepTwoExperimentMean": 2,
    r"\StepTwoExperimentMedian": 2,
    r"\StepTwoExperimentMax": 2,
    r"\StepTwoExperimentRatioMean": 2,
    r"\ExtractionFailures": 3,
    r"\ExtractionFailuresCount": 3,
    r"\ExtractionFailuresPercentage": 3,
    r"\FinalCanonicalModelVersions": 3,
    r"\FinalCanonicalModelVersionsPercentage": 3,
    r"\StepOneCommitFoldersWithCanonicals": 3,
    r"\StepThreeInputScope": 3,
    r"\StepThreeScopeLabel": 3,
}



# ---------------------------------------------------------------------------
# LaTeX variables.tex parsing helpers (used by reports)
# ---------------------------------------------------------------------------

_NEWCOMMAND_RE = re.compile(r"^\\newcommand\{\\([A-Za-z0-9_]+)\}\{(.*)\}\s*$")
_SUMMARY_NUM_LINE_RE = re.compile(
    r"^\s*(?P<label>[^:]+?)\s*:\s*(?P<count>-?[0-9][0-9,]*)\s*"
    r"(?:\((?P<pct>-?[0-9]+(?:\.[0-9]+)?)\s*%\))?\s*$"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _latex_macro_to_name(macro: str) -> str:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", macro)
    return spaced.replace("_", " ").strip().lower()


def _normalize_variable_name(name: str) -> str:
    cleaned = name.strip().lower()
    cleaned = re.sub(r"^step\d+\s+", "", cleaned)
    cleaned = re.sub(r"^step\s+(one|two|three|four|five|six|seven|eight|nine|ten)\s+", "", cleaned)
    return cleaned.strip()


def _collect_tex_files(path: Path) -> list[Path]:
    if path.is_dir():
        return sorted(path.glob("*.tex"))
    if path.is_file():
        return [path]
    return []


def _extract_tex_macros(path: Path) -> set[str]:
    macros: set[str] = set()
    for tex_file in _collect_tex_files(path):
        for line in tex_file.read_text(encoding="utf-8").splitlines():
            m = _NEWCOMMAND_RE.match(line.strip())
            if m:
                macros.add(f"\\{m.group(1)}")
    return macros


def get_reported_latex_macros() -> set[str]:
    """Every macro defined in the generated ``results/variables.tex``."""
    return _extract_tex_macros(VARIABLES_TEX_PATH)


def parse_variables_tex(path: Path = VARIABLES_TEX_PATH) -> list[dict[str, str | int | None]]:
    tex_files = _collect_tex_files(path)
    if not tex_files:
        return []
    reported_macros = get_reported_latex_macros()
    parsed: list[dict[str, str | int | None]] = []
    for tex_file in tex_files:
        for raw in tex_file.read_text(encoding="utf-8").splitlines():
            m = _NEWCOMMAND_RE.match(raw.strip())
            if not m:
                continue
            macro = m.group(1)
            value = m.group(2).strip()
            parsed.append(
                {
                    "name": _latex_macro_to_name(macro),
                    "latex_name": f"\\{macro}" if f"\\{macro}" in reported_macros else None,
                    "default_value": value,
                    "pipeline_step_number": LATEX_TO_PIPELINE_STEP.get(f"\\{macro}"),
                }
            )
    return parsed


# ---------------------------------------------------------------------------
# Database connections
# ---------------------------------------------------------------------------

def _resolve_repo_path(raw_repo_path: str, source_name: str) -> Path:
    raw = Path(raw_repo_path)
    if not raw.is_absolute():
        return SAM2026_ROOT / raw
    if raw.exists():
        return raw
    preferred = SOURCE_DIR / source_name
    if preferred.exists():
        return preferred
    if "source" in raw.parts:
        idx = raw.parts.index("source")
        return SAM2026_ROOT / Path(*raw.parts[idx:])
    return raw


def _remove_obsolete_run_settings(conn: sqlite3.Connection) -> None:
    """Drop settings rows that no step reads.

    Such a row is worse than no row: it reads like a knob, and turning it does
    nothing.
    """
    for step, key in REMOVED_PIPELINE_SETTINGS:
        conn.execute(
            "DELETE FROM run_settings WHERE pipeline_step_number = ? AND key = ?",
            (step, key),
        )


def _normalize_source_repo_paths(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT name, repo_path FROM step1_sources").fetchall()
    for row in rows:
        source_name = str(row["name"]).strip()
        raw_repo_path = str(row["repo_path"]).strip()
        candidate = Path(raw_repo_path)
        if not candidate.is_absolute() or candidate.exists():
            continue
        preferred_rel = Path("source") / source_name
        preferred_abs = SAM2026_ROOT / preferred_rel
        if preferred_abs.exists():
            conn.execute(
                "UPDATE step1_sources SET repo_path = ? WHERE name = ?",
                (str(preferred_rel), source_name),
            )
            continue
        parts = candidate.parts
        if "source" in parts:
            idx = parts.index("source")
            rel_guess = Path(*parts[idx:])
            guess_abs = SAM2026_ROOT / rel_guess
            if guess_abs.exists():
                conn.execute(
                    "UPDATE step1_sources SET repo_path = ? WHERE name = ?",
                    (str(rel_guess), source_name),
                )


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_step2_classes_connection() -> sqlite3.Connection:
    """Connection to the separate Step 2 class-listing database."""
    conn = sqlite3.connect(STEP2_CLASSES_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _rename_table_if_needed(conn: sqlite3.Connection, old: str, new: str) -> None:
    has_old = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (old,)
    ).fetchone()
    has_new = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (new,)
    ).fetchone()
    if has_old and not has_new:
        conn.execute(f"ALTER TABLE {old} RENAME TO {new}")


def init_step2_classes_db() -> None:
    with get_step2_classes_connection() as conn:
        _rename_table_if_needed(conn, "step2_enumerated_classes", "step2_classes")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS step2_classes (
                source_name TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                class_name TEXT NOT NULL,
                is_experiment INTEGER NOT NULL,
                PRIMARY KEY(source_name, commit_hash, class_name)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_s2c_source_experiment "
            "ON step2_classes(source_name, is_experiment)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_s2c_source_experiment_class "
            "ON step2_classes(source_name, is_experiment, class_name)"
        )


def init_database() -> None:
    """Create the SAM2026 pipeline schema (steps 1--3) and seed defaults."""
    with get_connection() as conn:
        # Migrate legacy table names if present.
        _rename_table_if_needed(conn, "step2_enumerated_classes", "step2_classes")
        _rename_table_if_needed(conn, "step3_extraction_failures", "step3_failures")
        # Migrate legacy column name failure_message -> compiler_message.
        cols = {r[1] for r in conn.execute("PRAGMA table_info(step3_failures)").fetchall()}
        if cols and "failure_message" in cols and "compiler_message" not in cols:
            conn.execute("ALTER TABLE step3_failures RENAME COLUMN failure_message TO compiler_message")

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS step1_sources (
                name TEXT PRIMARY KEY,
                repo_path TEXT NOT NULL,
                package_file TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                excluded_branches TEXT NOT NULL,
                default_branch TEXT NOT NULL,
                commit_cutoff_tag TEXT
            );

            CREATE TABLE IF NOT EXISTS run_settings (
                pipeline_step_number INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY(pipeline_step_number, key)
            );

            CREATE TABLE IF NOT EXISTS step3_sublibraries (
                class_name TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS run_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                step_name TEXT NOT NULL,
                generated_at_utc TEXT NOT NULL,
                duration_str TEXT NOT NULL DEFAULT '',
                run_settings_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS run_variables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                latex_name TEXT UNIQUE,
                pipeline_step_number INTEGER
            );

            CREATE TABLE IF NOT EXISTS run_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                variable_id INTEGER NOT NULL,
                value TEXT NOT NULL,
                UNIQUE(run_id, variable_id),
                FOREIGN KEY(run_id) REFERENCES run_logs(id) ON DELETE CASCADE,
                FOREIGN KEY(variable_id) REFERENCES run_variables(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS step1_commits (
                source_name TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                author_email TEXT NOT NULL,
                branches TEXT NOT NULL,
                commit_message TEXT NOT NULL,
                excluded INTEGER NOT NULL,
                exclusion_reason TEXT NOT NULL,
                PRIMARY KEY(source_name, commit_hash)
            );

            CREATE TABLE IF NOT EXISTS step1_files (
                source_name TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                mo_file_path TEXT NOT NULL,
                is_baseline INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(source_name, commit_hash, mo_file_path)
            );

            CREATE TABLE IF NOT EXISTS step2_classes (
                source_name TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                class_name TEXT NOT NULL,
                is_experiment INTEGER NOT NULL,
                PRIMARY KEY(source_name, commit_hash, class_name)
            );

            CREATE TABLE IF NOT EXISTS step2_enumeration_progress (
                source_name TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                enumerated_classes_count INTEGER NOT NULL DEFAULT 0,
                last_updated_utc TEXT NOT NULL,
                PRIMARY KEY(source_name, commit_hash)
            );

            CREATE TABLE IF NOT EXISTS step3_classes (
                source_name TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                class_name TEXT NOT NULL,
                is_experiment INTEGER NOT NULL,
                canonical_model_path TEXT NOT NULL,
                error_message TEXT NOT NULL,
                is_inside_sublibraries_list INTEGER NOT NULL,
                pilot_match_mode TEXT NOT NULL,
                matched_sublibrary TEXT NOT NULL,
                canonical_produced INTEGER NOT NULL,
                PRIMARY KEY(source_name, commit_hash, class_name)
            );

            CREATE TABLE IF NOT EXISTS step3_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                class_name TEXT NOT NULL,
                failure_type TEXT NOT NULL,
                compiler_message TEXT NOT NULL,
                created_at_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS step3_commit_progress (
                source_name TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                processed_classes_count INTEGER NOT NULL DEFAULT 0,
                saved_models_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_updated_utc TEXT NOT NULL,
                PRIMARY KEY(source_name, commit_hash)
            );
            """
        )

        _seed_run_variables_from_tex(conn)

        conn.executemany(
            """
            INSERT OR IGNORE INTO step1_sources
            (name, repo_path, package_file, enabled, excluded_branches, default_branch, commit_cutoff_tag)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            DEFAULT_SOURCES,
        )
        _remove_obsolete_run_settings(conn)
        conn.executemany(
            "INSERT OR IGNORE INTO run_settings(pipeline_step_number, key, value) VALUES (?, ?, ?)",
            [(int(s), k, "" if v is None else str(v)) for s, k, v in DEFAULT_PIPELINE_SETTINGS],
        )
        conn.executemany(
            "INSERT OR IGNORE INTO step3_sublibraries(class_name, enabled) VALUES (?, 1)",
            [(c,) for c in DEFAULT_PILOT_SUBLIBRARIES],
        )
        _normalize_source_repo_paths(conn)


# ---------------------------------------------------------------------------
# run_variables / run_summary helpers
# ---------------------------------------------------------------------------

def _ensure_run_variable(
    conn: sqlite3.Connection,
    *,
    name: str,
    pipeline_step_number: int | None,
    latex_name: str | None = None,
) -> int:
    name = _normalize_variable_name(name)
    mapped_step = LATEX_TO_PIPELINE_STEP.get(latex_name) if latex_name else None
    effective_step = mapped_step if mapped_step is not None else pipeline_step_number

    if latex_name:
        row = conn.execute("SELECT id FROM run_variables WHERE latex_name = ?", (latex_name,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO run_variables(name, latex_name, pipeline_step_number) VALUES (?, ?, ?)",
                (name, latex_name, effective_step),
            )
        else:
            conn.execute(
                "UPDATE run_variables SET name = ?, pipeline_step_number = COALESCE(?, pipeline_step_number) WHERE id = ?",
                (name, effective_step, int(row["id"])),
            )
    else:
        row = conn.execute(
            "SELECT id FROM run_variables WHERE name = ? AND pipeline_step_number IS ? AND latex_name IS NULL",
            (name, effective_step),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO run_variables(name, latex_name, pipeline_step_number) VALUES (?, NULL, ?)",
                (name, effective_step),
            )

    row = conn.execute(
        """
        SELECT id FROM run_variables
        WHERE (latex_name = ? AND ? IS NOT NULL)
           OR (name = ? AND pipeline_step_number IS ? AND ? IS NULL AND latex_name IS NULL)
        ORDER BY id DESC
        LIMIT 1
        """,
        (latex_name, latex_name, name, effective_step, latex_name),
    ).fetchone()
    return int(row["id"])


def _seed_run_variables_from_tex(conn: sqlite3.Connection) -> None:
    for item in parse_variables_tex():
        _ensure_run_variable(
            conn,
            name=str(item["name"]),
            latex_name=(str(item["latex_name"]) if item.get("latex_name") else None),
            pipeline_step_number=(int(item["pipeline_step_number"])
                                  if item.get("pipeline_step_number") is not None else None),
        )


def _insert_run_summary_value(
    conn: sqlite3.Connection,
    run_id: int,
    value: str,
    *,
    name: str,
    pipeline_step_number: int | None,
    latex_name: str | None = None,
) -> None:
    latex: str | None = None
    if latex_name:
        maybe = latex_name if latex_name.startswith("\\") else "\\" + latex_name.lstrip("\\")
        reported_macros = get_reported_latex_macros()
        latex = maybe if maybe in reported_macros else None
    fallback_name = name or (_latex_macro_to_name(latex.lstrip("\\")) if latex else "metric")
    variable_id = _ensure_run_variable(
        conn,
        name=fallback_name,
        latex_name=latex,
        pipeline_step_number=pipeline_step_number,
    )
    conn.execute(
        """
        INSERT INTO run_summary(run_id, variable_id, value)
        VALUES (?, ?, ?)
        ON CONFLICT(run_id, variable_id) DO UPDATE SET value = excluded.value
        """,
        (int(run_id), variable_id, str(value)),
    )


def _serialize_run_settings(run_settings: dict[str, object] | None) -> str:
    if not run_settings:
        return "{}"
    try:
        return json.dumps(run_settings, sort_keys=True)
    except TypeError:
        return json.dumps({str(k): str(v) for k, v in run_settings.items()}, sort_keys=True)


def start_run_log(
    source_name: str,
    step_name: str,
    generated_at_utc: str | None = None,
    run_settings: dict[str, object] | None = None,
) -> int:
    init_database()
    generated = generated_at_utc or utc_now()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO run_logs(source_name, step_name, generated_at_utc, duration_str, run_settings_json)
            VALUES (?, ?, ?, '', ?)
            """,
            (source_name, step_name, generated, _serialize_run_settings(run_settings)),
        )
        return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def update_run_duration(run_id: int, duration_str: str) -> None:
    with get_connection() as conn:
        conn.execute("UPDATE run_logs SET duration_str = ? WHERE id = ?", (duration_str, run_id))


def set_run_values(
    run_id: int,
    values_by_latex_name: dict[str, str | int | float],
    names_by_latex_name: dict[str, str] | None = None,
) -> None:
    if not values_by_latex_name:
        return
    names = names_by_latex_name or {}
    with get_connection() as conn:
        for latex_name, value in values_by_latex_name.items():
            _insert_run_summary_value(
                conn,
                run_id,
                str(value),
                latex_name=latex_name,
                name=names.get(latex_name, ""),
                pipeline_step_number=LATEX_TO_PIPELINE_STEP.get(latex_name),
            )


def get_latest_value_for_latex(latex_name: str) -> str | None:
    init_database()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT rs.value
            FROM run_summary rs
            JOIN run_variables rv ON rv.id = rs.variable_id
            JOIN run_logs rl ON rl.id = rs.run_id
            WHERE rv.latex_name = ?
            ORDER BY rl.generated_at_utc DESC, rl.id DESC
            LIMIT 1
            """,
            (latex_name,),
        ).fetchone()
    return str(row["value"]) if row is not None else None


def get_enabled_sources() -> list[SourceConfig]:
    init_database()
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT name, repo_path, package_file, enabled, excluded_branches,
                   default_branch, commit_cutoff_tag
            FROM step1_sources
            WHERE enabled = 1
            ORDER BY name
            """
        ).fetchall()
    return [
        SourceConfig(
            name=row["name"],
            repo_path=_resolve_repo_path(str(row["repo_path"]), str(row["name"])),
            package_file=row["package_file"],
            enabled=bool(row["enabled"]),
            excluded_branches=[b for b in row["excluded_branches"].split(";") if b],
            default_branch=row["default_branch"],
            commit_cutoff_tag=row["commit_cutoff_tag"],
        )
        for row in rows
    ]


def get_setting(step_number: int, key: str, default: str = "") -> str:
    init_database()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT value FROM run_settings WHERE pipeline_step_number = ? AND key = ?",
            (int(step_number), key),
        ).fetchone()
    return str(row["value"]) if row is not None else default


def get_setting_int(step_number: int, key: str) -> int | None:
    value = get_setting(step_number, key, "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def get_setting_bool(step_number: int, key: str, default: bool = False) -> bool:
    value = get_setting(step_number, key, "1" if default else "0").strip().lower()
    return value in {"1", "true", "yes", "on"}


def format_duration(seconds: float) -> str:
    if seconds < 0:
        return "0 s"
    if seconds < 60:
        return f"{seconds:.1f} s".rstrip("0").rstrip(".")
    if seconds < 3600:
        return f"{seconds / 60:.1f} m".rstrip("0").rstrip(".")
    return f"{seconds / 3600:.1f} hr".rstrip("0").rstrip(".")


# Read once at import time; consumed by pipeline/1_filter_commits.py.
COMMIT_AGE_LIMIT_YEARS: int | None = get_setting_int(1, "COMMIT_AGE_LIMIT_YEARS")

