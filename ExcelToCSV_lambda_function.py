import json
import boto3
import pandas as pd
import io
from urllib.parse import unquote_plus


def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    if event:
        s3_record = event['Records'][0]['s3']
        bucket_name = str(s3_record['bucket']['name'])
        key_name = unquote_plus(str(s3_record['object']['key']))
        file_name = key_name.split('/')[-1]

        file_obj = s3.get_object(Bucket=bucket_name, Key=key_name)
        file_content = file_obj['Body'].read()
        read_excel_data = io.BytesIO(file_content)
        print(bucket_name, key_name)

        df = pd.read_excel(read_excel_data, sheet_name='AutoPopulate')
        df = df[df['ID'].notnull()]     # remove null in rows
        df.columns = df.columns.str.strip()
        df = df.rename(columns={'Org Name': 'Name',
                                'Calendar Month': 'Month',
                                'Calendar Year': 'Year',
                                'Count of Unique': 'Count',
                                'Total Number of Encounters': 'Encounters',
                                'Total Appointments Recorded': 'Appointments'
                                })
        print(df.head())

        file_name_new = file_name.replace('.xlsx', '.csv')
        print(file_name_new)
        df.to_csv(f'/tmp/{file_name_new}', index_label='row_number')

        s3_resource.meta.client.upload_file(f'/tmp/{file_name_new}', bucket_name, f'output/{file_name_new}')

    return {
        'statusCode': 200,
        'body': json.dumps(f'uploaded csv file location - output/{file_name_new}')
    }
