"""Step 0: wipe previous-run data so the pipeline can start fresh.

Removes:
  * dataset/pipeline.db (and -wal/-shm sidecars)
  * dataset/step2_classes.db (and sidecars)
  * dataset/canonical_models/* (per-source canonical .mo trees)
  * worktrees/* (per-worker git worktrees)

The DB schemas are re-created automatically by settings.init_database()
on the next pipeline run, so deleting the .db files is safe.

Usage:
    python pipeline/0_cleanup_data.py          # interactive confirm
    python pipeline/0_cleanup_data.py --yes    # non-interactive
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


from settings import (  # noqa: E402
    CANONICAL_MODELS_BASE_DIR,
    DB_PATH,
    SAM2026_ROOT,
    STEP2_CLASSES_DB_PATH,
)

WORKTREES_DIR = SAM2026_ROOT / "worktrees"


def _db_files(db_path: Path) -> list[Path]:
    return [db_path, db_path.with_suffix(db_path.suffix + "-wal"),
            db_path.with_suffix(db_path.suffix + "-shm")]


def _remove_file(path: Path) -> None:
    if path.exists() or path.is_symlink():
        path.unlink()
        print(f"[OK]  removed file       {path}")


def _clear_directory_contents(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()
        print(f"[OK]  removed            {child}")


def _path_size_bytes(path: Path) -> int:
    try:
        if path.is_symlink() or path.is_file():
            return path.lstat().st_size
        if path.is_dir():
            total = 0
            for p in path.rglob("*"):
                try:
                    total += p.lstat().st_size
                except OSError:
                    pass
            return total
    except OSError:
        pass
    return 0


def _format_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024.0 or unit == "TB":
            return f"{size:7.2f} {unit}"
        size /= 1024.0
    return f"{num_bytes} B"


def _summary() -> list[tuple[str, int]]:
    items: list[tuple[str, int]] = []
    for db in (DB_PATH, STEP2_CLASSES_DB_PATH):
        for f in _db_files(db):
            if f.exists():
                items.append((str(f), _path_size_bytes(f)))
    for base in (CANONICAL_MODELS_BASE_DIR, WORKTREES_DIR):
        if base.exists():
            for child in base.iterdir():
                items.append((str(child), _path_size_bytes(child)))
    return items


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompt.")
    args = parser.parse_args()

    targets = _summary()
    print("=" * 60)
    print("  Step 0 - Cleanup previous run data")
    print("=" * 60)
    if not targets:
        print("[INFO] Nothing to clean.")
        return

    print(f"About to delete {len(targets)} item(s):")
    total = 0
    for path_str, size in targets:
        print(f"  [{_format_size(size)}]  {path_str}")
        total += size
    print(f"  {'-' * 58}")
    print(f"  [{_format_size(total)}]  total")

    if not args.yes:
        answer = input("\nProceed? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            print("[ABORT] No changes made.")
            return

    for db in (DB_PATH, STEP2_CLASSES_DB_PATH):
        for f in _db_files(db):
            _remove_file(f)

    _clear_directory_contents(CANONICAL_MODELS_BASE_DIR)
    _clear_directory_contents(WORKTREES_DIR)

    CANONICAL_MODELS_BASE_DIR.mkdir(parents=True, exist_ok=True)
    WORKTREES_DIR.mkdir(parents=True, exist_ok=True)

    print("\n[DONE] Cleanup complete. Re-run the pipeline to recreate DB schemas.")


if __name__ == "__main__":
    main()

