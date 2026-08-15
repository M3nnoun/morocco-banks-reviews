# Morocco Bank Reviews — Analytics Pipeline

An end-to-end data pipeline over Google Maps reviews of Moroccan bank branches: scrape, clean,
enrich with French-language NLP, and model into a star schema for BI reporting.

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Scrapy](https://img.shields.io/badge/Scrapy-60a839)
![spaCy](https://img.shields.io/badge/spaCy-09A3D5?logo=spacy&logoColor=white)

> **Repository status:** the architecture below is fully implemented, but several source files are
> missing from version control and the repository is **not runnable as cloned**. See
> [Repository state](#repository-state) before attempting to use it.

## Problem

Moroccan banks accumulate thousands of Google Maps reviews across hundreds of branches, almost all
written in French. Individually they are anecdotes; in aggregate they are a branch-level customer
satisfaction signal that nobody is reading.

This pipeline turns that unstructured text into a queryable fact table that answers questions like:
which branches underperform their bank's average, which topics drive negative sentiment in a given
city, and how a bank ranks against competitors within a region.

## Architecture

```
Google Maps
    │  Scrapy + Playwright
    ▼
PostgreSQL  public.reviews                       raw scraped reviews
    │  dbt (dbt_project)
    ▼
stg_reviews → cleaned_reviews                    cleaning, date parsing, dedup
    │  Python NLP (scripts/index.py)
    ▼
enriched_reviews                                 sentiment, LDA topics, confidence
    │  dbt (star_schema_dbt)
    ▼
dim_bank · dim_branch · dim_location · dim_region · dim_topic · dim_sentiment
    │
    ▼
fact_reviews → fact_reviews_complete             BI-ready fact table
    │
    ▼
Looker Studio
```

### 1. Ingestion — Scrapy + Playwright

Google Maps renders reviews client-side and lazy-loads them on scroll, so a plain HTTP scraper
returns nothing useful. The scraper uses **scrapy-playwright** to drive a real browser, letting
Scrapy handle scheduling, throttling and retries while Playwright handles rendering.

Output lands in `public.reviews`: `id`, `city`, `business_name`, `address`, `review_text`, `stars`,
`timestamp`, `scraped_at`.

### 2. Cleaning — `dbt_project`

`stg_reviews` stages the raw table. `cleaned_reviews` does the substantive work:

- **Relative date parsing.** Google Maps returns timestamps as French relative strings — *"il y a 3
  mois"*, *"il y a un an"*, *"il y a 2 semaines"*. The model resolves each form to a real date in
  SQL, handling the implicit-singular case (*"il y a un mois"* has no digit to extract) and
  rejecting absurd offsets. Without this the entire time dimension is unusable.
- **Deduplication.** `ROW_NUMBER()` over bank, branch, review text, rating and city, keeping the
  most recent — the scraper revisits branches and Google itself serves duplicates.
- **Text normalisation.** Lowercase, punctuation stripped, whitespace collapsed, then French
  stopwords removed by unnesting the text to words and filtering against an inline stopword list.
- **Junk filtering.** Drops placeholder rows (`'No review text found'`), reviews under six
  characters, and rows with no resolvable city.
- **Rating validation.** Ratings outside 0–5 become `NULL` rather than silently skewing averages.

`reviews_nlp_analysis` adds a pure-SQL enrichment layer via dbt macros — rule-based language
detection (fr/en/de/es/it by diacritics and function words), sentiment, and topic tagging into a
`topics` array plus boolean flag columns. This path is self-contained in the warehouse and serves as
a fallback to the Python enrichment below.

### 3. Enrichment — `scripts/index.py`

The main NLP stage, operating on `cleaned_reviews` and writing `enriched_reviews`:

- **Preprocessing** — spaCy `fr_core_news_sm` lemmatisation, keeping only nouns, adjectives and
  verbs, filtered against French stopwords extended with banking-domain terms (*banque*, *agence*,
  *faire*, *aller*…) that are frequent but carry no signal here.
- **Sentiment** — a hybrid score: 70% TextBlob-FR (`PatternAnalyzer`) polarity, 30% a hand-built
  French lexicon of banking-specific positive and negative terms, thresholded at ±0.1 into
  positif / négatif / neutre. The lexicon compensates for TextBlob-FR's weak coverage of
  colloquial Moroccan French review vocabulary.
- **Topic modelling** — TF-IDF over unigrams to trigrams (3,000 features) into a 15-topic LDA. Each
  review gets its dominant topic plus a confidence score.
- **Topic naming** — LDA returns numbered topics, which are useless in a dashboard. Topics are
  matched against a keyword→theme map covering 15 banking themes (*Temps d'Attente Excessif*,
  *Services Guichet Automatique*, *Frais et Tarification*, *Personnel Problématique*…), with a
  contextual fallback that composes a name from the top keywords when no theme matches.

`scripts/enrich_reviews.py` is an earlier iteration — 7 fixed topic labels, no confidence scores,
no hybrid sentiment. Kept for reference.

### 4. Dimensional modelling — `star_schema_dbt`

`stg_bank_reviews` assigns surrogate keys and extracts postal codes; dimensions are conformed for
bank, branch, location, region, topic and sentiment.

`fact_reviews` computes the analytical layer:

- Satisfaction level and a normalised 0–1 satisfaction score
- Full temporal breakdown — year, quarter, month, ISO weekday, weekend flag, review age, freshness bucket
- **Contextual averages via window functions** — mean rating per bank, per branch, per location, per
  topic, computed alongside each row so relative performance needs no self-join
- **Relative performance** — each review's delta against its bank, location and topic average
- **Sentiment/rating coherence** — flags reviews where NLP sentiment contradicts the star rating,
  a useful check on the sentiment model itself
- A composite score weighting rating 70% and topic confidence 30%

`fact_reviews_complete` denormalises every dimension in and adds regional roll-ups: market share per
bank within a region, bank rank within region, rating standard deviation and coefficient of
variation per bank, and a branch maturity classification by review volume. It is built wide and
pre-aggregated deliberately — Looker Studio performs far better against one flat table than against
a live star join.

## Repository structure

```
scrap-google-maps/       Scrapy + Playwright scraper (git submodule reference — see below)
dbt_project/             Cleaning and SQL-based NLP models
star_schema_dbt/         Dimensional model: dimensions, fact tables, marts
scripts/
  index.py               Main NLP enrichment (spaCy, TextBlob-FR, LDA)
  enrich_reviews.py      Earlier enrichment iteration
  backup.sql             Database backup
  views_only.sql         View definitions
requirements.txt
```

## Repository state

These are real gaps, listed so nobody wastes time cloning and running it:

- **The scraper is not present.** `scrap-google-maps` is recorded as a git submodule (gitlink
  `2319c6e`) but the repository has no `.gitmodules` file, so the directory clones empty and the
  reference cannot be resolved.
- **dbt model sources are not tracked.** Only `target/` — dbt's compiled output — is committed. The
  `models/*.sql` sources, `dbt_project.yml`, macros and `profiles.yml` are absent. The compiled SQL
  in `target/compiled/` documents exactly what each model does and is the best available reference,
  but `dbt run` cannot work without the sources.
- **`scripts/main.py` and `scripts/pull_data.py` are missing**, leaving only their Windows
  `Zone.Identifier` metadata files in the index.
- **`Zone.Identifier` files are tracked** throughout `scripts/` — Windows alternate-data-stream
  artefacts that should never have been committed.
- **Build artefacts and logs are committed** — `target/`, `logs/`, `dbt.log`,
  `review_analysis.log` — because the repository has no `.gitignore`.
- **Database credentials are hardcoded** in `scripts/index.py` and `scripts/enrich_reviews.py`
  (`ReviewAnalysisConfig.DATABASE_URI`), as is an absolute Linux path for NLTK data. Both should
  move to environment variables.

## Setup

Given the above, the pipeline cannot currently be reproduced end to end. For the parts that are
present:

```bash
git clone https://github.com/M3nnoun/morocco-banks-reviews.git
cd morocco-banks-reviews
pip install -r requirements.txt
python -m spacy download fr_core_news_sm
```

The enrichment scripts expect a PostgreSQL database named `google_reviews_db` containing a
`public_public.cleaned_reviews` table, with the connection string set in
`ReviewAnalysisConfig.DATABASE_URI`. `scripts/backup.sql` may be usable to restore a schema.

## Tech stack

**Ingestion:** Scrapy, scrapy-playwright, Playwright
**Warehouse:** PostgreSQL
**Transformation:** dbt
**NLP:** spaCy, TextBlob-FR, NLTK, scikit-learn (TF-IDF, LDA)
**Processing:** pandas, NumPy, SQLAlchemy
**BI:** Looker Studio
