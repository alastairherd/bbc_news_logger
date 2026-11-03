# BBC News Analyser

An independent archive of the BBC News front page and Most Read list, captured hourly since April
2025, with a research site for exploring it.

[Open the analyser](https://alastairherd.github.io/bbc_news_logger/) ·
[Browse the dataset](https://huggingface.co/datasets/AlastairH/bbc-news-logger) ·
[Read the methodology](https://alastairherd.github.io/bbc_news_logger/methodology/)

Not affiliated with or endorsed by the BBC. BBC content remains subject to the BBC's terms and
copyright.

## What it does

Each hour a collector records the ten promoted front-page slots and the ten Most Read places. A
daily job fetches every new article, DeepSeek V4.1 Flash labels it with themes and a short
summary, and BGE Small embeddings connect related coverage across months.

The site is static. Overview compares the latest capture with the one before it. Explore is the
full story archive. Trends follows themes and recurring stories over time, with a weekly summary
of the most-covered themes. Promotion vs attention compares the two lists story by story: which
front-page stories reach Most Read, which reach it without front-page promotion, and how long it
takes. Ask the archive searches by meaning in the browser, matches query concepts against an
archive ontology, and can request a short answer that cites both the articles it found and
archive-wide theme or entity statistics.

Labels, clusters and answers come from models and can be wrong.

## What it shows so far

I started this to see whether front-page promotion drives what ends up in Most Read, and which kinds
of story make one list but not the other. Across 18,545 stories captured between April 2025 and
October 2026:

- 72% of stories promoted on the front page also reached Most Read. Nearly two thirds of those
  appeared on both lists in the same hourly capture, and only 3% took more than three hours.
- 30% of stories in Most Read were never captured on the front page.
- About one in five promoted football and World Cup stories reached Most Read, and Premier League
  stories almost never did.
- UK politics was the reverse. Most of the UK politics stories in Most Read never appeared on the
  front page.
- “DIY store closes as travellers move on to car park” spent 35 hourly captures in Most Read without
  being promoted, and “How coffee chains like Costa lost the matcha generation” reached number one
  the same way. “Murder investigation after man's death” sat on the front page for 97 captures and
  never entered Most Read.

These are list appearances, not audience figures, and “the front page” means the ten promoted
slots the collector records. The
[Promotion page](https://alastairherd.github.io/bbc_news_logger/promotion/) has the full breakdown
by theme and month.

## How it works

```text
BBC News homepage ──► GitHub Actions ──► Hugging Face (Parquet)
                                                │
                              DuckDB at build time
                                                ▼
                                  Static Astro site on GitHub Pages
                                                │
                     Ask the archive ──► Cloudflare Worker ──► DeepSeek
```

Scheduled GitHub Actions jobs collect, fetch and label the stories, then publish Parquet files to
Hugging Face. Each Pages build reads those tables with DuckDB and writes small JSON files that the
site loads directly; search runs in a browser worker against a quantised vector index and a static
ontology mart. A Cloudflare Worker handles the two things that need a server: starting the hourly
collection, because GitHub's scheduler dropped too many runs, and making the capped DeepSeek call
behind Ask the archive.

## How it was built

The project started as a scraper writing CSV files into this repository. Before adding anything
else I made the data reliable. The collector now fails instead of publishing a partial capture,
each story has a stable ID based on its canonical URL, and the old CSV, ZIP and Parquet files were
migrated with hashes and row counts that can be checked.

As the archive grew I moved the data from GitHub to Hugging Face, which is designed for versioned
datasets and lets people query the tables directly. I hadn't realised that GitHub disables
scheduled workflows in a public repository after 60 days without activity, and the hourly data
commits had been that activity. Once they stopped, GitHub switched the schedules off and collection
stopped for six weeks in autumn 2026. A monthly keepalive commit now prevents that.

The architecture follows Spicy Data's
[“A live data app for $0”](https://spicydata.ai/blog/zero-dollar-data-app/), which argues for
precomputing with DuckDB and serving a static Astro site instead of running a BI tool. I wanted
something that could run unattended for close to nothing, so it suited me. Ask the archive is
modelled on the
[Fenic Hacker News agent example](https://github.com/typedef-ai/fenic-examples/tree/main/hn_agent)
and ideas from NVIDIA's [Auto Ontology](https://github.com/NVIDIA/auto-ontology): search finds the
articles and matching archive concepts first, and the model can only answer from those numbered
sources and must cite them.

I chose DeepSeek V4 Flash for labelling because it was far cheaper than comparable models for the
quality, and moved to V4.1 Flash when it replaced V4. Models have kept improving since then, and
newer options such as Haiku 5.5 or Luna 6 might now suit the job, but I have not tried them
because a fair comparison means paying to relabel a large part of the archive.

I put a hard ceiling on every paid path, mainly to protect my bank balance: labelling stops at $1 a
month, weekly summaries at $0.25 a month and public answers at $0.50 a day. Scheduled model work
runs in DeepSeek's cheaper off-peak window.

So that a crash never means paying twice for the same work, each labelling response is saved to a
local checkpoint before the next request goes out, and the saved results are pushed to the dataset
in batches. If a run dies halfway, the next one carries on from what has already been paid for.
Articles that failed are only retried when I ask.

I worked out the visual design with Impeccable, a design plugin for coding agents. I wanted the
site to look like BBC News, red included, while making it clear that it is not a BBC product.

All of the code was written with Codex and Claude Code. I decided what to build and how far the
site's claims could go, and checked each result against the real data before moving on. A few
examples:

- The Trends page froze the browser. Profiling showed it was building 26,521 theme options and
  rescanning 70,685 rows on the main thread. Moving the indexing into a Web Worker and listing only
  the most active themes fixed it.
- Nothing on the site showed the six-week outage, or that GitHub had been running the hourly job
  only every few hours. A Cloudflare cron now starts collection, and a GitHub issue is opened if no
  capture lands for three hours.
- I added Fenic after reading about it, to run an MCP research service and the weekly theme
  summaries. It didn't suit this project. The service was never hosted, and at most eight summaries a
  week didn't justify the dependency, so I removed it. The summaries now use the same DeepSeek client as
  the labelling.
- To get outside feedback, I had Codex send groups of GPT-6.1 Sol agents through the live site,
  each acting as a different visitor: a researcher, a journalist on deadline, a newcomer, a phone
  user and a keyboard user. A second round asked real questions and checked each cited answer
  against its source. They found answers that treated the date an article was fetched as the date
  of the event, and a race condition that could attach an answer to the wrong query's sources. Both
  are fixed, along with most of their usability notes.

### Why build an ontology instead of relying on raw ranked retrieval?

Originally, **Ask the archive** and the trend charts relied directly on the raw labels returned by
the language model and the top twenty articles returned by vector search. That ran into four
problems across a 20,000-article archive:

1. **Label fragmentation across thousands of daily calls.** Because articles are labelled in small
   daily batches, the model invents slightly different strings for the same concept over time:
   `"UK_politics"`, `"British politics"` and `"UK politics"`, or `"Sir Keir Starmer"`,
   `"Prime Minister Keir Starmer"` and `"Keir Starmer"`, or `"Prince Andrew"` and
   `"Andrew Mountbatten-Windsor"`. Counting raw strings splits one person or theme across several
   rows and undercounts every longitudinal trend.
2. **Sample blindness in top-20 vector search.** Vector similarity returns the twenty closest
   articles to a question, which is good for finding examples and poor for answering archive-wide
   questions (*“How often do NHS waiting-list stories reach Most Read?”* or *“How many stories has
   the archive captured on Keir Starmer?”*). In the old setup, the answer model could only count
   within its twenty-article sample, or one large continuing story could fill most of the twenty
   slots and crowd out the rest of the year's coverage.
3. **Too flat a jump from 11 topics to 2,360 themes.** Top-level topics (`politics`, `world`,
   `health`) are too broad to compare editorial promotion against reader attention, while most of
   the 2,360 fine-grained themes appear in fewer than thirty paired captures—too small a sample to
   calculate a reliable promotion reach rate on their own.
4. **Ambiguous names and abbreviations.** Short strings such as `"US"`, `"UN"`, `"Met"`, `"ICE"` or
   `"Chelsea"`, and compound phrases such as `"prison reform"` or `"labour market"`, easily collide
   with stopwords or unrelated political parties (`"Reform UK"`, `"Labour Party"`) under naive
   substring matching.

Adapting the reconciliation and semantic-layer ideas from NVIDIA's
[Auto Ontology](https://github.com/NVIDIA/auto-ontology) to fit a static, zero-server site, the
pipeline now adds a governed ontology layer on top of the immutable Parquet shards:

- **Deterministic read-time reconciliation (`ontology.py`).** Stored `semantic/signals` shards are
  never rewritten. Instead, when marts, clusters and weekly summaries are built, synonyms, plural
  variants, institutional abbreviations and honorifics are folded into canonical theme and entity
  names with deterministic tie-breaking, while preserving institutional titles (*“Archbishop of
  Canterbury”*, *“Mayor of Paris”*) and place names (*“The Hague”*, *“The Gambia”*).
- **A 46-category mid-level taxonomy (`semantic/taxonomy`).** Every canonical theme appearing in
  five or more stories is mapped incrementally into one of 46 controlled parent categories (such as
  *UK government & Westminster*, *NHS & healthcare services*, *Middle East conflict*, or
  *Migration, asylum & borders*). That gives the **Trends** and **Promotion vs attention** pages a
  complete mid-level view where all 46 categories clear the 30-story statistical floor, and lets
  smaller themes in **Ask the archive** inherit their parent category's promotion baseline.
- **Wikidata entity linking (`semantic/wikidata`).** The top 400 recurring entities are linked
  incrementally to Wikidata Q-IDs (`Q145`, `Q6383803`), entity types (`person`, `organisation`,
  `location`, `event`) and short descriptions, disambiguating people, places and institutions and
  connecting the archive to external datasets.
- **Concept-aware hybrid retrieval and dual citations.** In the browser worker, queries are scanned
  from 4-word phrases down to single words on strict token boundaries before blending with BGE
  vector similarity, and each recurring event cluster is capped at four results in the top twenty so
  one story cannot monopolise the evidence. Matched theme (`[T1]`), entity (`[E1]`) and
  continuing-story (`[R1]`) records are shown above the search results and passed to DeepSeek
  alongside the twenty articles (`[1]`–`[20]`), so synthesized answers can cite individual reports
  for specific claims and ontology records for archive-wide totals and promotion rates.

## Reading the data

- A Most Read rank is not an audience count, and a front-page slot is not a measure of editorial
  importance. Collection covers one BBC News web page, not apps or regional editions.
- 6–21 May 2025: Most Read was not captured. Promotion vs attention leaves these captures out.
- 27 August – 28 October 2026: GitHub ran collection every three to six hours instead of hourly.
  Compare counts from this period per capture, not per day.
- 17 September – 27 October 2026: no collection, because GitHub had disabled the schedules. This
  period cannot be backfilled.
- Captures before 28 October 2026 stored some headlines with broken characters (“Â£” for “£”). The
  site repairs them on display; the dataset has not yet been repaired.

The [methodology](https://alastairherd.github.io/bbc_news_logger/methodology/) has the full data
contract, migration repairs and clustering rules.

## Dataset

| Configuration | One row per |
| --- | --- |
| `observations` | story position in a capture |
| `article_snapshots` | fetched article version |
| `scrape_runs` | collection attempt |
| `story_signals` | labelled article version |
| `article_embeddings` | embedded article version |
| `recurring_events` | article in a recurring-story cluster |
| `theme_taxonomy` | canonical theme mapped to its mid-level parent category |
| `entity_wikidata` | recurring named entity linked to Wikidata (`qid`, type, description) |

Article HTML is in a separate
[raw dataset](https://huggingface.co/datasets/AlastairH/bbc-news-logger-raw) so ordinary analysis
does not download it.

## Run locally

Requires Python 3.10–3.12, [uv](https://docs.astral.sh/uv/) and Node.js 22.

```bash
uv sync --extra dev
uv run pytest -q

# Build the site data from the public dataset, then start the site
uv run bbc-news build-marts --output web/public/data
cd web && npm ci && npm run dev
```

Other commands:

```bash
uv run bbc-news scrape                 # parse and validate a live capture without publishing
uv run bbc-news scrape --upload        # publish it (needs HF_TOKEN)
uv run bbc-news compact-dataset --publish
./scripts/refresh_semantics.sh 1000    # embed and label up to 1,000 new article versions
uv run python -m bbc_news_logger.jobs.weekly --dry-run
```

## Scheduled jobs

| Job | When |
| --- | --- |
| Collect observations | Hourly at :07, started by the Cloudflare Worker |
| Alert on stalled collection | When the Worker finds no capture for three hours |
| Fetch article snapshots | Daily, 02:17 UTC |
| Embed, label and cluster | Daily, 12:23 UTC (DeepSeek off-peak) |
| Weekly theme summaries | Mondays, 12:47 UTC |
| Compact the dataset | Weekly |
| Deploy the site | Every three hours and on relevant pushes |

A monthly workflow commits a timestamp so GitHub does not disable the schedules on a quiet
repository.

## Repository layout

```text
src/bbc_news_logger/   collector, publication, migration, marts and scheduled jobs
web/                   Astro site and browser workers
workers/research/      Cloudflare Worker for collection dispatch and answers
tests/                 Python tests
datasets/              Hugging Face dataset cards
.github/workflows/     scheduled jobs, CI and deployment
```

---

This README was written with help from Claude Code and edited by me.
