## Overview
Ideascrape scrapes indie hacker websites then runs it through a series of processing code then finally uploads it to supabase. While processing, it creates vector embeddings. 

Was my first big project when trying to be an entrepreneur, ended up being a massive time sink. Lots of learnings though.


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
