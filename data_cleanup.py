"""Archive old scraper outputs so the live data tree stays small.

The scrapers need only the current day (and the article fetcher needs yesterday's
CSV logs), so older daily files can be compressed into monthly zip archives.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable, Sequence

DATA_DIR = Path("data")
DEFAULT_RETENTION_DAYS = 90

CSV_PATTERNS = {
    "bbc_most_read": re.compile(r"^bbc_most_read_(\d{4}-\d{2}-\d{2})\.csv$"),
    "bbc_front_page_promos": re.compile(r"^bbc_front_page_promos_(\d{4}-\d{2}-\d{2})\.csv$"),
}
ARTICLE_CONTENT_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})\.parquet$")


@dataclass(frozen=True)
class CleanupCandidate:
    """A dated data file that can be moved into a monthly archive."""

    path: Path
    date: dt.date
    category: str
    archive_name: str


@dataclass(frozen=True)
class CleanupSummary:
    """Counts returned by the cleanup routine for logs and tests."""

    archived_files: int
    removed_files: int
    archive_paths: tuple[Path, ...]
    dry_run: bool


def parse_iso_date(value: str) -> dt.date:
    """Parse an ISO date from a scraper filename."""

    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def cutoff_date(retention_days: int, today: dt.date | None = None) -> dt.date:
    """Return the oldest date that should remain unarchived."""

    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")
    today = today or dt.datetime.now(dt.timezone.utc).date()
    return today - dt.timedelta(days=retention_days)


def archive_path_for(data_dir: Path, candidate: CleanupCandidate) -> Path:
    """Return the monthly zip path for a cleanup candidate."""

    return (
        data_dir
        / "archive"
        / candidate.category
        / f"{candidate.date:%Y}"
        / f"{candidate.date:%m}.zip"
    )


def iter_cleanup_candidates(data_dir: Path, cutoff: dt.date) -> Iterable[CleanupCandidate]:
    """Yield daily CSV and article-content parquet files older than cutoff."""

    if not data_dir.exists():
        return

    for path in sorted(data_dir.glob("*.csv")):
        for category, pattern in CSV_PATTERNS.items():
            match = pattern.match(path.name)
            if not match:
                continue
            file_date = parse_iso_date(match.group(1))
            if file_date < cutoff:
                yield CleanupCandidate(
                    path=path,
                    date=file_date,
                    category=category,
                    archive_name=path.relative_to(data_dir).as_posix(),
                )
            break

    article_dir = data_dir / "article-content"
    if not article_dir.exists():
        return

    for path in sorted(article_dir.glob("*.parquet")):
        match = ARTICLE_CONTENT_PATTERN.match(path.name)
        if not match:
            continue
        file_date = parse_iso_date(match.group(1))
        if file_date < cutoff:
            yield CleanupCandidate(
                path=path,
                date=file_date,
                category="article-content",
                archive_name=path.relative_to(data_dir).as_posix(),
            )


def write_archive(archive_path: Path, candidates: Sequence[CleanupCandidate]) -> None:
    """Write candidates into an archive, replacing matching entries if present."""

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    replacement_names = {candidate.archive_name for candidate in candidates}

    with NamedTemporaryFile(
        prefix=f".{archive_path.name}.",
        suffix=".tmp",
        dir=archive_path.parent,
        delete=False,
    ) as tmp:
        temp_path = Path(tmp.name)

    try:
        with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as output_zip:
            if archive_path.exists():
                with zipfile.ZipFile(archive_path, "r") as existing_zip:
                    for info in existing_zip.infolist():
                        if info.filename in replacement_names:
                            continue
                        output_zip.writestr(info, existing_zip.read(info.filename))

            for candidate in sorted(candidates, key=lambda item: item.archive_name):
                output_zip.write(candidate.path, candidate.archive_name)

        temp_path.replace(archive_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def cleanup_data(
    data_dir: Path = DATA_DIR,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    today: dt.date | None = None,
    dry_run: bool = False,
) -> CleanupSummary:
    """Archive old files and remove the now-compressed originals."""

    data_dir = Path(data_dir)
    cutoff = cutoff_date(retention_days, today)
    candidates = list(iter_cleanup_candidates(data_dir, cutoff))
    archive_groups: dict[Path, list[CleanupCandidate]] = {}
    for candidate in candidates:
        archive_groups.setdefault(archive_path_for(data_dir, candidate), []).append(candidate)

    for archive_path, grouped_candidates in sorted(archive_groups.items()):
        logging.info(
            "%s %d file(s) into %s",
            "Would archive" if dry_run else "Archiving",
            len(grouped_candidates),
            archive_path,
        )
        if not dry_run:
            write_archive(archive_path, grouped_candidates)

    removed = 0
    for candidate in candidates:
        logging.info(
            "%s %s",
            "Would remove" if dry_run else "Removing archived source",
            candidate.path,
        )
        if not dry_run:
            candidate.path.unlink()
            removed += 1

    return CleanupSummary(
        archived_files=len(candidates),
        removed_files=0 if dry_run else removed,
        archive_paths=tuple(sorted(archive_groups)),
        dry_run=dry_run,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive old BBC scraper data files into monthly zip files."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help="Data directory to clean up (default: data).",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Keep this many recent days as loose files (default: {DEFAULT_RETENTION_DAYS}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be archived without changing files.",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = build_parser().parse_args()
    summary = cleanup_data(
        data_dir=args.data_dir,
        retention_days=args.retention_days,
        dry_run=args.dry_run,
    )
    logging.info(
        "%s %d file(s) across %d archive(s); removed %d source file(s).",
        "Would archive" if summary.dry_run else "Archived",
        summary.archived_files,
        len(summary.archive_paths),
        summary.removed_files,
    )


if __name__ == "__main__":
    main()
