import os, pathlib, glob

def fix_json_files(temp_path: str, source_path: str):
  """
    Fix json files and write in a temp path before start
  """
  os.makedirs(temp_path, exist_ok=True)
  source_path_filter_regex = fr'{source_path}/*.json'
  for source_json_path in glob.glob(source_path_filter_regex, recursive=True):
    file_name = source_json_path.split('/')[-1]
    with open(source_json_path, 'r') as source_json:
      current_json_temp_path = f'{temp_path}/{file_name}'
      with open(current_json_temp_path, 'w') as temp_json:
        temp_json.write(source_json.read().replace('}{', '}\n{'))
        temp_json.close()
        source_json.close()

def remove_temp_json_files(temp_path: str):
  """
    Remove all json files at temp path
  """
  temp_path_filter_regex = fr'{temp_path}/*.json'
  for temp_file in glob.glob(temp_path_filter_regex, recursive=True):
    pathlib.Path(temp_file).unlink()
