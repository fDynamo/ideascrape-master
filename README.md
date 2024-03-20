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
