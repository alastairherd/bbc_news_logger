import datetime as dt
import zipfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_cleanup import cleanup_data, iter_cleanup_candidates


def write_file(path: Path, text: str = "content") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def test_cleanup_archives_old_daily_files_by_category_and_month(tmp_path):
    data_dir = tmp_path / "data"
    write_file(data_dir / "bbc_most_read_2024-01-01.csv", "rank,title\n1,old")
    write_file(data_dir / "bbc_front_page_promos_2024-01-02.csv", "title\nold")
    write_file(data_dir / "article-content" / "2024-01-03.parquet", "parquet-ish")
    write_file(data_dir / "bbc_most_read_2024-04-15.csv", "rank,title\n1,new")
    write_file(data_dir / "notes.txt", "ignore me")

    summary = cleanup_data(
        data_dir=data_dir,
        retention_days=90,
        today=dt.date(2024, 4, 15),
    )

    assert summary.archived_files == 3
    assert summary.removed_files == 3
    assert not (data_dir / "bbc_most_read_2024-01-01.csv").exists()
    assert not (data_dir / "bbc_front_page_promos_2024-01-02.csv").exists()
    assert not (data_dir / "article-content" / "2024-01-03.parquet").exists()
    assert (data_dir / "bbc_most_read_2024-04-15.csv").exists()
    assert (data_dir / "notes.txt").exists()

    with zipfile.ZipFile(data_dir / "archive" / "bbc_most_read" / "2024" / "01.zip") as archive:
        assert archive.read("bbc_most_read_2024-01-01.csv") == b"rank,title\n1,old"
    with zipfile.ZipFile(data_dir / "archive" / "bbc_front_page_promos" / "2024" / "01.zip") as archive:
        assert archive.read("bbc_front_page_promos_2024-01-02.csv") == b"title\nold"
    with zipfile.ZipFile(data_dir / "archive" / "article-content" / "2024" / "01.zip") as archive:
        assert archive.read("article-content/2024-01-03.parquet") == b"parquet-ish"


def test_cleanup_dry_run_does_not_change_files(tmp_path):
    data_dir = tmp_path / "data"
    old_file = data_dir / "bbc_most_read_2024-01-01.csv"
    write_file(old_file)

    summary = cleanup_data(
        data_dir=data_dir,
        retention_days=30,
        today=dt.date(2024, 3, 1),
        dry_run=True,
    )

    assert summary.archived_files == 1
    assert summary.removed_files == 0
    assert old_file.exists()
    assert not (data_dir / "archive").exists()


def test_iter_cleanup_candidates_skips_cutoff_date(tmp_path):
    data_dir = tmp_path / "data"
    write_file(data_dir / "bbc_most_read_2024-01-31.csv")
    write_file(data_dir / "bbc_most_read_2024-02-01.csv")

    candidates = list(iter_cleanup_candidates(data_dir, dt.date(2024, 2, 1)))

    assert [candidate.path.name for candidate in candidates] == ["bbc_most_read_2024-01-31.csv"]
