import auth
import requests
import logging
import pandas as pd
import csv
from io import StringIO


BASE_URL = "https://api.anaplan.com/2/0/workspaces/"

def get_export_id_with_name(token, workspace_id, model_id, export_name):
    headers = auth.token_headers(token)

    url = BASE_URL + workspace_id + "/models/"+ model_id +"/exports"

    response = requests.get(url, headers=headers)

    found_id = None
    logging.debug(response.json())
    for export in response.json()['exports']:
        if export_name == export['name']:
            found_id=export['id']
            break

    return found_id

def run_export_with_name(token, workspace_id, model_id, export_name):
    export_id = get_export_id_with_name(token, workspace_id, model_id, export_name)

    headers= auth.token_headers(token)

    start_url = BASE_URL + workspace_id + "/models/" + model_id + "/exports/" + export_id + "/tasks"

    print(start_url)

    response = requests.post(start_url, headers=headers, data='{"localeName": "en_US"}')
    print(response.json())
    task_id = response.json()['task']['taskId']
    wait_export_completed(token, workspace_id, model_id, export_id, task_id)

    file = load_file_with_name(token, workspace_id, model_id, export_name)

    return file

def wait_export_completed(token, workspace_id, model_id, export_id, task_id):
    headers = auth.token_headers(token)

    url = BASE_URL + workspace_id + "/models/" + model_id + "/exports/" + export_id + "/tasks/" + task_id

    while True:
        response = requests.get(url, headers=headers)
        if response.json()['task']['taskState'] == "COMPLETE":
            break
        logging.debug("waiting on " + workspace_id+" " + model_id + " " + task_id + " to complete. Current status " + response.json()['task']['taskState'])

def get_file_info_with_name(token, workspace_id, model_id, name):
    headers = auth.token_headers(token)

    url = BASE_URL + workspace_id + "/models/" + model_id + "/files/"

    response = requests.get(url, headers=headers)
    found_info = None
    for file in response.json()['files']:
        if file['name'] == name:
            found_info = file
            break
    print(response.json())
    return found_info

def load_file_with_name(token, workspace_id, model_id, name):
    file_info = get_file_info_with_name(token,workspace_id, model_id, name)

    octet_headers = auth.token_headers(token,"octet")
    json_headers = auth.token_headers(token)

    url = BASE_URL + workspace_id + "/models/" + model_id + "/files/" + file_info['id'] + "/chunks"

    chunks_response = requests.get(url, headers=json_headers)

    chunks = chunks_response.json()
    print(chunks)
    column_names = None
    data = []

    for i, chunk in enumerate(chunks['chunks']):
        chunkID = chunk['id']
        print(f'Getting chunk {chunkID}')
        getChunk = requests.get(url + f'/{chunkID}', headers=octet_headers)
        csv_text = getChunk.content.decode('utf-8')  # Assuming CSV is encoded in UTF-8
        rows = csv.reader(StringIO(csv_text))
        # Drop the first row if it's the first chunk
        if i == 0:
            column_names = next(rows)
        data.extend(rows)
        print(data)
        print(column_names)
        print('Status code: ' + str(getChunk.status_code))

    df = pd.DataFrame(data, columns=column_names)

    return df

def get_import_info_with_name(token, workspace_id, model_id, name):
    headers = auth.token_headers(token)

    url = BASE_URL + workspace_id + "/models/" + model_id + "/imports/"

    response = requests.get(url, headers=headers)
    found_info = None
    for import_action in response.json()['imports']:
        if import_action['name'] == name:
            found_info = import_action
            break
    print(response.json())
    return found_info

def post_df_file_single_chunk(token, workspace_id, model_id, file_id, df):
    url = BASE_URL + workspace_id + "/models/" + model_id + "/files/" + file_id
    headers = auth.token_headers(token, "octet")

    data_file = df.to_csv()

    file_upload = requests.put(url,headers=headers,data=(data_file))

    if file_upload.ok:
        print('File Upload Successful.')
    else:
        print('There was an issue with your file upload: '
              + str(file_upload.status_code))

def run_import(token, workspace_id, model_id, import_id):
    url = BASE_URL + workspace_id + "/models/" + model_id + "/imports/" + import_id + "/tasks"
    headers = auth.token_headers(token)
    response = requests.post(url, headers=headers, data='{"localeName": "en_US"}')
    print(response.status_code)

def import_df_with_names(token, workspace_id, model_id, name, df):
    import_action = get_import_info_with_name(token, workspace_id, model_id, name)
    print(import_action)
    post_df_file_single_chunk(token, workspace_id, model_id, import_action['importDataSourceId'], df)
    run_import(token, workspace_id, model_id, import_action['id'])