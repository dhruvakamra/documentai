import sys
import os
import google.auth
import pandas as pd
import re
import base64
import json
import argparse
import time
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud import documentai_v1beta3 as documentai

#---------------------------------------------------------------------------------------------------------------------
######### Initialize variables #########

###------ Project Variables ------###
project_id = 'dk-cloudfusion-demo'                                       # Your project-id
###------ GCS Variables ------###
gcs_output_bucket = 'dk_docai_temp_bucket'
gcs_output_csv_bucket = 'dk_docai_invoice_csv_archives'
###------ BQ Variables ------###
bq_dataset = 'document_ai'
invoice_table = 'invoice_data'
inventory_table = 'inventory_sold'
###------ Script Variables ------###
pubsub_topic = 'docai-topic'
subscription_id = 'docai-topic-sub'

#---------------------------------------------------------------------------------------------------------------------
def triggered(event, context):
    print("This Function was triggered by messageId {} published at {} to {}".format(context.event_id, context.timestamp, context.resource["name"]))
    from concurrent.futures import TimeoutError
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    def callback(message):
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    if 'data' in event:
        blob_name = base64.b64decode(event['data']).decode('utf-8')
        print(f"1. Received message on topic *{pubsub_topic}* and subscription *{subscription_id}* ... message data -->> {blob_name}. ACK sent!")
        process_blob(blob_name)
    else:
        print(f"1. No data found in found in {context.event_id}")

def upload_pd_as_csv_to_gcs(bucket_name, pdframe, out_file_name):
    print(f"** Uploading data as CSV to output bucket >> {gcs_output_csv_bucket}")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.blob(out_file_name).upload_from_string(pdframe.to_csv(index=False), 'text/csv')
    print(f"->> Upload completed. Creating schema Bigquery ingest for *{out_file_name}* ...")

def load_csv_to_bq(table_name, schema, csv):
    bq_client = bigquery.Client()      #Construct a BigQuery client object.
    table_id = f'{project_id}.{bq_dataset}.{table_name}'
    print(f"** Checking if *{table_id}* exists in Bigquery. If yes, will append rows. If not, will create table and add rows")
    output_uri = f'gs://{gcs_output_csv_bucket}/{csv}'

    load_job = bq_client.load_table_from_uri(output_uri, table_id, job_config=schema)  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print(f'->> Insert row complete. {destination_table.num_rows} rows now available in table {table_name}')


def process_blob(blob_name):
    # Results are written to GCS. Use a regex to find output files
    match = re.match(r"([^/]+)/(.+)", blob_name)
    prefix = match.group(1) + '/'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_output_bucket)
    blob_list = list(bucket.list_blobs(prefix=prefix))

    for i, blob in enumerate(blob_list):
    # If JSON file, download the contents of this blob as a bytes object.
        if ".json" in blob_name:
            blob_as_bytes = blob.download_as_string()
            print(f'3. Fetched file {i} >> {blob_name}')
            print("4. Downloading blob as bytes for tranformation...")
            document = documentai.types.Document.from_json(blob_as_bytes)
            # For a full list of Document object attributes, please reference this page:
            # https://cloud.google.com/document-ai/docs/reference/rpc/google.cloud.documentai.v1beta3#document
            # Read the entities output from the processor
            types = []
            values = []
            confidence = []
                
            for entity in document.entities:
                types.append(entity.type_)
                values.append(entity.mention_text)
                confidence.append(round(entity.confidence,4))

            # Create a Pandas Dataframe to print the values in tabular format. 
            df = pd.DataFrame({'type': types, 'value': values, 'confidence': confidence})
            transform_data(df)
            i=i+1
            print(f"Load completed, deleting temp. blob >> {blob} and shutting down...")
            blob.delete()     
        else:
            print(f"Skipping non-supported file type {blob.name}")
            i=i+1
    


def transform_data(df: dict):
    #Normalize Data
    print("5. Beginning data normalization and transformation...")
    df_t = df.rename(columns={'type':'index'}).drop(columns=['confidence']).T
    df_t.columns = df_t.iloc[0]
    df_t = df_t.drop(df_t.index[0])
    df_t = df_t.reset_index(drop=True)
    # transform amount columns and create 
    for num_col in [col for col in df_t.columns if '_amount' in col]:
        df_t[num_col] = pd.to_numeric(df_t[num_col].replace({'\$':'', ',':''}, regex = True))

    # transform data
    df_t['due_date'] = pd.to_datetime(df_t['due_date'])
    df_t['invoice_date'] = pd.to_datetime(df_t['invoice_date'])
    df_t['receiver_name'] = df_t['receiver_name'].str.replace(",",'')
    df_t['receiver_address'] = df_t['receiver_address'].replace('\\n',' ', regex=True)

    print("6. Data tranformation completed.")
    ### Define Schema for invoice table and export csv for ingest and archival in GCS
    keeper_cols = ['invoice_id','invoice_date','due_date','purchase_order','supplier_name','receiver_tax_id','receiver_name',"receiver_address",'total_amount','total_tax_amount','net_amount','freight_amount']
    df_t = df_t[keeper_cols]
    out_file_name = invoice_table + '_' + time.strftime("%m%d%Y_%H%M%S") + '.csv'
    upload_pd_as_csv_to_gcs(gcs_output_csv_bucket, df_t, out_file_name)
    
    job_config_invoice = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('invoice_id', 'STRING'),
            bigquery.SchemaField('invoice_date', 'DATE'),
            bigquery.SchemaField('due_date', 'DATE'),
            bigquery.SchemaField('purchase_order', 'STRING'),
            bigquery.SchemaField('supplier_name', 'STRING'),
            bigquery.SchemaField('receiver_tax_id', 'STRING'),
            bigquery.SchemaField('receiver_name', 'STRING'),
            bigquery.SchemaField('receiver_address', 'STRING'),
            bigquery.SchemaField('total_tax_amount', 'FLOAT'),
            bigquery.SchemaField('freight_amount', 'FLOAT'),
            bigquery.SchemaField('net_amount', 'FLOAT'),
            bigquery.SchemaField('total_amount', 'FLOAT'),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    load_csv_to_bq(invoice_table ,job_config_invoice, out_file_name)

    #---------------------------------------------------------------------------------------------------------------------
    ### Inventory Data / line item --- add processor results to a Pandas Dataframe and transform for BQ ingestion
    add_row = []

    line_item_values = df.loc[df["type"] == 'line_item', "value"]
    line_items= line_item_values.to_numpy()

    var = df_t['invoice_id'].iloc[0]
    
    for value in line_items:
        new_val = value.replace("$","")
        x = re.match(r"([\W\w\s]*)\s(\d+)\s([\W\w]*)\s([\W\w]*)", new_val)
        if len(add_row) == 0:
            add_row = [(var, x.group(1), x.group(2), x.group(3), x.group(4))]
        else:
            add_row.extend([(var, x.group(1), x.group(2), x.group(3), x.group(4))])

    ### Define Schema for inventory table and export csv for ingest and archival in GCS
    df2 = pd.DataFrame(add_row, columns=['invoice_id', 'item_description', 'item_quantity', 'item_unit_price', 'item_total'])
    out_file_name = inventory_table + '_' + time.strftime("%m%d%Y_%H%M%S") + '.csv'
    upload_pd_as_csv_to_gcs(gcs_output_csv_bucket, df2, out_file_name)

    job_config_inventory = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('invoice_id', 'STRING'),
            bigquery.SchemaField('item_description', 'STRING'),
            bigquery.SchemaField('item_quantity', 'FLOAT'),
            bigquery.SchemaField('item_unit_price', 'FLOAT'),
            bigquery.SchemaField('item_total', 'FLOAT'),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    load_csv_to_bq(inventory_table, job_config_inventory, out_file_name)
