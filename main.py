import pathlib
from pismo.transform.event import EventTransform
from pismo.utils.json import fix_json_files, remove_temp_json_files

SOURCE_PATH = f'{pathlib.Path().resolve()}/ingest_bucket'
TEMP_PATH = f'{pathlib.Path().resolve()}/temp_bucket'
DESTINATION_PATH = f'{pathlib.Path().resolve()}/output_bucket'

print('Starting event tranform and write')

fix_json_files(TEMP_PATH, SOURCE_PATH)

EventTransform(TEMP_PATH, DESTINATION_PATH).split_and_write()

remove_temp_json_files(TEMP_PATH)

print('Finished event tranform and write')
