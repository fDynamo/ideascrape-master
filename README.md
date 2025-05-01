## Overview
Ideascrape (AKA Hackersearch) is a search engine for indie products. The data is prepared by scraping indie product websites (think product hunt), then processing that data with some python code, then finally uploading it to supabase. The NextJS website serves as the front end for the app. 

While processing, it structures raw data, creates vector embeddings for search, etc.

Was my first big project when trying to be an entrepreneur, ended up being a massive time sink. Lots of learnings.


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

## Common pipeline args

-r: retry, supply name of pipeline run to retry
-t: use test pipeline folder
--resetTest: resets test folder

## Daily carthago run

```
python pipeline_definitions/carthago.py -n --prod --upsync
```
