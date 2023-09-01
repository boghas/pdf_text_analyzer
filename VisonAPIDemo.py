import os
import re
import json
import openai
import pandas as pd
import openpyxl
from google.cloud import vision_v1, storage
from google.oauth2 import service_account


def initialize_demo(google_application_credentials, openai_api_key_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_application_credentials
    openai.api_key = openai_api_key_path

    if os.environ['GOOGLE_APPLICATION_CREDENTIALS'] and openai.api_key:
        print('Initialized successfully')
    else:
        print('Initializing failed')


def upload_to_bucket(project=None, bucket=None, file=None):
    key_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    try:
        credentials = service_account.Credentials.from_service_account_file(key_file)
        scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])
        
        client = storage.Client(credentials=credentials, project=project)
        bucket_to_upload = client.get_bucket(bucket)

        blob = bucket_to_upload.blob(file.split('/')[1])
        blob.upload_from_filename(file)

        print(f'File: {file} uploaded to bucket')
        print(f'The pulic url: {blob.public_url}')
        file_uri = f'gs://{bucket}/{os.path.basename(file)}'
        print(f'file URI: {file_uri}')

        return file_uri
    except Exception as e:
        print(e)


def convert_pdf_to_text(file_uri, batch_size=4):
    client = vision_v1.ImageAnnotatorClient()

    out_blob_name = file_uri.split('/')[-1].split('.')[0]

    mime_type = 'application/pdf'
    feature = vision_v1.types.Feature(
        type=vision_v1.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source_uri = file_uri
    gcs_source = vision_v1.types.GcsSource(uri=gcs_source_uri)
    input_config = vision_v1.types.InputConfig(gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination_uri = f'gs://hollerlaw_demo_bucket/{out_blob_name}'
    gcs_destination = vision_v1.types.GcsDestination(uri=gcs_destination_uri)
    output_config = vision_v1.types.OutputConfig(gcs_destination=gcs_destination, batch_size=batch_size)

    async_request = vision_v1.types.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config, output_config=output_config)

    operation = client.async_batch_annotate_files(requests=[async_request])
    operation.result(timeout=180)

    storage_client = storage.Client()
    match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)
    bucket = storage_client.get_bucket(bucket_name)

    # List object with the given prefix
    blob_list = list(bucket.list_blobs(prefix=prefix))
    return blob_list

    
def call_chat_gpt_api(predefined_gpt_question, message):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": predefined_gpt_question + message}]
    )

    return completion


def parse_openai_response(response):
    return_message = response["choices"][0]["message"]["content"]
    return return_message


def write_to_xlsx(xlsx_name, columns_to_write):
    print('Writing to excel')
    if os.path.exists(xlsx_name):
        print('Excel already exists!')
        pass
    else:
        df = pd.DataFrame([['Recording Jurisdiction', 'Return to', 'Client', 'Book/Page', 'Recorded Date']])
        df2 = pd.DataFrame(columns_to_write)
        df = df._append(df2)
        print(df)
        df.to_excel(xlsx_name)


if __name__ == "__main__":
    '''Add your own credentials'''
    google_application_credentials = 'resources/mindit-ai-playground-2ca342c3899e.json'
    openai_api_key_path = 'resources/openai-key.txt'
    project = 'mindit ai playground'
    bucket = 'hollerlaw_demo_bucket'
    source_folder = 'resources/'
    xlsx_name = 'resources/output_demo.xlsx'
    results = []

    predefined_questions = ['What is the county name in this text? Reply only with the county name. ', \
                            'Who should I return to after the recording? It should be specified after the line "Return to". \
                                Reply only with the firm name. If there are more firms, give me all of them. ', \
                            'Give me the grantor name or company in this text. Reply only with the grantor name ', \
                            'Give me the book and page numbers from this text. It should be written after the first line BK: . Ignore the the other lines with BK: ', \
                            'Give me the recording date. Not the date when it was signed. Reply only with the date and no other additional message. ']

    initialize_demo(google_application_credentials, openai_api_key_path)

    for file in os.listdir(source_folder):
        if file.endswith('.pdf'):
            message = ''
            print(f'Uploading {file} to {bucket}')
            file_uri = upload_to_bucket(project, bucket, os.path.join(source_folder, file))
            print('Converting PDF to text...')
            out_blob_list = convert_pdf_to_text(file_uri)

            print('Converted text file:')
            for blob in out_blob_list:
                if file.split('.')[0] in blob.name and '.json' in blob.name:
                    print(blob.name)
                    json_string = blob.download_as_text()
                    response = json.loads(json_string)

                    for page_nr in range(len(response['responses'])):
                        page_response = response['responses'][page_nr]
                        annotation = page_response['fullTextAnnotation']

                        print('Converted text:')
                        print(annotation['text'])

                        message += annotation['text']
            print('-------------------------------------------------')
            print(message)
            gpt_answers_as_list = []
            
            for predefined_question in predefined_questions:
                gpt_answer = call_chat_gpt_api(predefined_question, message)
            
                result = parse_openai_response(gpt_answer)
                gpt_answers_as_list.append(result)

                print(result)
            
            results.append(gpt_answers_as_list)

    write_to_xlsx(xlsx_name, columns_to_write=results)





