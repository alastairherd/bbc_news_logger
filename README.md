# BBC News Most Read Logger

This project automatically scrapes the "Most Read" stories from the BBC News homepage (`https://www.bbc.co.uk/news`) every hour and logs them.

## How it Works

A Python script (`BBC_News_Most_Read_Scraper.py`) uses `requests` and `BeautifulSoup4` to fetch and parse the news homepage. The top 10 most read stories (title and link) are extracted.

A GitHub Actions workflow (`.github/workflows/scrape_bbc.yml`) runs this script every hour.

## Data Storage

The scraped data is stored in CSV files within the `data/` directory.
- A new file is created each day, named `bbc_most_read_YYYY-MM-DD.csv`.
- Each file contains entries for all scrapes performed on that UTC date.
- Columns: `timestamp` (UTC time of scrape), `rank` (1-10), `title`, `link`.

## Setup

1. Clone the repository.
2. Ensure Python 3.x is installed.
3. Install dependencies with [uv](https://github.com/astral-sh/uv):
 ```bash
 uv pip install -e .
 ```

## Running Manually (Optional)

You can run the scraper manually:
```bash
python BBC_News_Most_Read_Scraper.py
```
This will append data to the current day's CSV file in the `data/` directory.

To fetch the full text of articles for yesterday's URLs:
```bash
python article_content_scraper.py
```

## Automation

The process is automated via GitHub Actions, running hourly and committing updated data files back to the repository.

## Daily Article Content Fetch

Each day at 02:00 UTC, the union of all URLs seen in the "Most Read" and front page promo logs from the previous day is fetched.  The full article HTML and a plain-text version are written to `data/article-content/{YYYY-MM-DD}.parquet`.
