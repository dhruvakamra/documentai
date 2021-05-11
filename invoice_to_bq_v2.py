import sys
import os
import google.auth
import pandas as pd
import re
import numpy as np
import simplejson as json #simplejson-3.17.2
from google.cloud import storage
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import bigquery


### Initialize variables#######
projectid = "dk-cloudfusion-demo"                                       # Your project-id
location = "us"                                                         # Format is 'us' or 'eu'
processorid = "b1b76efccbab279b"                                        # Create processor in Cloud Console

GCS_INPUT_BUCKET = 'dk-docai-bucket'
GCS_INPUT_PREFIX = 'invoice_drop/'
GCS_OUTPUT_URI = 'gs://dk-docai-bucket'
GCS_OUTPUT_URI_PREFIX = 'results'
TIMEOUT = 300

output_bucket="dk-docai-bucket"
bq_dataset = "document_ai"
invoice_table = "invoice_data"
inventory_table = "inventory_sold"

def upload_pd_as_csv_to_gcs(bucket_name, pdframe, out_file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.blob(out_file_name + '.csv').upload_from_string(pdframe.to_csv(index=False), 'text/csv')


def load_csv_to_bq(import_file, schema):
    bq_client = bigquery.Client()      #Construct a BigQuery client object.
    table_id = f'{projectid}.{bq_dataset}.{import_file}'
    output_uri = f'gs://{output_bucket}/{import_file}.csv'

    load_job = bq_client.load_table_from_uri(output_uri, table_id, job_config=schema)  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows".format(destination_table.num_rows) + " to " + table_id)


def process_document_sample():
    # Instantiates a client
    client_options = {"api_endpoint": "{}-documentai.googleapis.com".format(location)}
    client = documentai.DocumentProcessorServiceClient(client_options=client_options)
    storage_client = storage.Client()
    
    # Sample invoices are stored in gs://cloud-samples-data/documentai/async_invoices/
    blobs = storage_client.list_blobs(GCS_INPUT_BUCKET, prefix=GCS_INPUT_PREFIX)
    input_configs = []
    print("Input Files:")
    for blob in blobs:
        if ".pdf" in blob.name:
            source = "gs://{bucket}/{name}".format(bucket = GCS_INPUT_BUCKET, name = blob.name)
            print(source)
            input_config = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
                gcs_source=source, mime_type="application/pdf")
            input_configs.append(input_config)

    destination_uri = f"{GCS_OUTPUT_URI}/{GCS_OUTPUT_URI_PREFIX}/"
    # Where to write results
    output_config = documentai.types.document_processor_service.BatchProcessRequest.BatchOutputConfig(
        gcs_destination=destination_uri
    )

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first.
    name = f"projects/{projectid}/locations/{location}/processors/{processorid}"
    request = documentai.types.document_processor_service.BatchProcessRequest(
        name=name,
        input_configs=input_configs,
        output_config=output_config,
    )

    operation = client.batch_process_documents(request)

    # Wait for the operation to finish
    #operation.result(timeout=TIMEOUT)

    # Results are written to GCS. Use a regex to find
    # output files

    match = re.match(r"gs://([^/]+)/(.+)", destination_uri)
    output_bucket = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(output_bucket)
    blob_list = list(bucket.list_blobs(prefix=prefix))

    for i, blob in enumerate(blob_list):
        # If JSON file, download the contents of this blob as a bytes object.
        if ".json" in blob.name:
            blob_as_bytes = blob.download_as_string()
            print("downloaded")

            document = documentai.types.Document.from_json(blob_as_bytes)
            print(f"Fetched file {i}")

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

                
        else:
            print(f"Skipping non-supported file type {blob.name}")
            i=i+1


def transform_data(df: dict):
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

    ### Define Schema for invoice table and export csv for ingest and archival in GCS
    keeper_cols = ['invoice_id','invoice_date','due_date','purchase_order','supplier_name','receiver_tax_id','receiver_name',"receiver_address",'total_amount','total_tax_amount','net_amount','freight_amount']
    df_t = df_t[keeper_cols]
    upload_pd_as_csv_to_gcs(output_bucket, df_t, invoice_table)

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
    load_csv_to_bq(invoice_table ,job_config_invoice)

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
    upload_pd_as_csv_to_gcs(output_bucket, df2, inventory_table)
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
    load_csv_to_bq(inventory_table , job_config_inventory)
    

process_document_sample()




