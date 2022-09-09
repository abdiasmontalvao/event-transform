import pathlib, glob
from pismo.transform.event import EventTransform

SOURCE_PATH = f'{pathlib.Path().resolve()}/ingest_bucket'
TEMP_PATH = f'{pathlib.Path().resolve()}/temp_bucket'
DESTINATION_PATH = f'{pathlib.Path().resolve()}/output_bucket'

def fix_json_files():
  """
    Fix json files and write in a temp path before start
  """
  source_path_filter_regex = fr'{SOURCE_PATH}/*.json'
  for source_json_path in glob.glob(source_path_filter_regex, recursive=True):
    file_name = source_json_path.split('/')[-1]
    with open(source_json_path, 'r') as source_json:
      current_json_temp_path = f'{TEMP_PATH}/{file_name}'
      with open(current_json_temp_path, 'w') as temp_json:
        temp_json.write(source_json.read().replace('}{', '}\n{'))
        temp_json.close()
        source_json.close()

def remove_temp_json_files():
  """
    Remove all json files at temp path
  """
  temp_path_filter_regex = fr'{TEMP_PATH}/*.json'
  for temp_file in glob.glob(temp_path_filter_regex, recursive=True):
    pathlib.Path(temp_file).unlink()

print('Starting event tranform and write')

fix_json_files()

EventTransform(TEMP_PATH, DESTINATION_PATH).split_and_write()

remove_temp_json_files()

print('Finished event tranform and write')
