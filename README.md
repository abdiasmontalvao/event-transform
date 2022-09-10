# event-transform

![Production workflow](https://github.com/abdiasmontalvao/event-transform/actions/workflows/merge.yaml/badge.svg?branch=main)

## About
This utility can read json files in a directory, remove duplicated events, split event by type and write partitioned by year, month and day using apache parquet.

## Getting started
Create a python file and import `EventTransform`:
```python
from pismo.transform.event import EventTransform
```

The `EventTransform` class constructor method can receive three arguments:
```python
event_transform = EventTransform(source_path: str, destination_path: str, write_mode: str)
```

| Argument | Description | Default value | Is mandatory |
|:---------|:------------|:--------------|:-------------|
| source_path | The source path uri where your data is located, can be an S3 URI too (but in this case you need to set your aws cli default credentials with read access to this bucket). | None | Yes |
| destination_path | The target path uri where your data will be written, can be an S3 URI too (but in this case you need to set your aws cli default credentials with write access to this bucket). | None | Yes |
| write_mode | Spark write mode, [click here](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes) to check Spark documentation. | 'append' | No |

Since you have an `EventTransform` instance, you can call the method `split_and_write()` to transform an write your events:
```python
event_transform.split_and_write()
```

## Handling transformation over non standard JSON files
If your JSON files have no line breaks between the rows, you can use this and util function to fix the JSON files before start:
```python
from pismo.transform.event import EventTransform
from pismo.utils.json import fix_json_files, remove_temp_json_files

SOURCE_PATH      = '/your/source/data/path'
TEMP_PATH        = '/temp/path/to/fix/your/json/files'
DESTINATION_PATH = '/path/where/your/data/will/be/written/'

# This function will fix your json files and write they to you temp path
fix_json_files(TEMP_PATH, SOURCE_PATH)

# Run your EventTransform trasnformation using your temp path as source path
EventTransform(TEMP_PATH, DESTINATION_PATH).split_and_write()

# Remove your temporary fixed JSON files
remove_temp_json_files(TEMP_PATH)
```

## Example: How to try this utility locally
Install dependencies:
```sh
~ pip install -r requirements.txt
```

Run the example usage code:
```sh
~ python main.py
```

The main file contains an example usage, it reads all jsons in **ingest_bucket**, do the transformations and write in **output_bucket**.

## Runing unit tests
```sh
~ pytest -v
```
