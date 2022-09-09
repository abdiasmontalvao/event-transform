# event-transform

## About
This utility can read json files in a directory, remove duplicated events, split event by type and write partitioned by year, month and day using apache parquet.

## How to try this utility locally
Install dependencies:

```~ pip install -r requirements.txt```

Run the example usage code:

```~ python main.py```

The main file contains an example usage, it reads all jsons in **ingest_bucket**, do the transformations and write in **output_bucket**.

## Runing unit tests
```~ pytest -v```
