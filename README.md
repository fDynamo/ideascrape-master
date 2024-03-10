## Setup

1. Create python venv in .venv
2. Add this to the bottom of activate script:

```
export PYTHONPATH="<path to project folder>"
export PYTHONUNBUFFERED=TRUE
```

3. `pip install -r requirements.txt`
4. `npm install`
5. Add to .env:

```
IDEASCRAPE_CACHE_FOLDER=
RUN_ARTIFACTS_FOLDER=
OPENAI_API_KEY=
```

## First carthago run

- AIFT scrapes: 18.81 mins
  - 0.07 for lists
  - 18.74 mins for posts
- PH scrapes: 0.43 mins
  - Only for like 5 items, but usually AIFT scrapes > PH so that's bottleneck
- Indiv scrape: 53.42 mins
- Similarweb scrape: 73.13 mins

Total for scraping alone: 145.8 mins, or around 2.43 hours

## Daily carthago run

```
python pipeline_orchestrators/carthago_run.py --prod-env --upload -n
```
