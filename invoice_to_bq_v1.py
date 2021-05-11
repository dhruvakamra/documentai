import sys
import os
import google.auth
import pandas as pd
import re
import numpy as np
from google.cloud import storage
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import bigquery

### Initialize variables#######
projectid = "<your-project-name>"                                       			# Your project-id
location = "us"                                                         			# Format is 'us' or 'eu'
processorid = "<invoice-processor-id>"                                        			# Create processor in Cloud Console
sample_invoice = "<location-of-file-on-localdisk>" 						# The local file in your current working directory
output_bucket="<output-bucket-name>"								# GCS Bucket where output CSVs will be uploaded
bq_dataset = "<your-bq-dataset>"
invoice_table = "invoice_data"
inventory_table = "inventory_sold"

#---------------------------------------------------------------------------------------------------------------------

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

#---------------------------------------------------------------------------------------------------------------------

### Instantiate processor 
processor_name = f'projects/{projectid}/locations/{location}/processors/{processorid}'
#gcs_source = documentai.types.GcsSource(uri=input_uri)

with open(sample_invoice, 'rb') as image:
    document = {'content': image.read(), 'mime_type': 'application/pdf'}
    request = {'name': processor_name, 'document': document}    
### Capture processor results 
results = documentai.DocumentProcessorServiceClient().process_document(request=request)

#---------------------------------------------------------------------------------------------------------------------

### Invoice Data --- add processor results to a Pandas Dataframe and transform for BQ ingestion
results_frame = [[entity.type_, entity.mention_text, round(entity.confidence, 4)] for entity in results.document.entities]
df = pd.DataFrame(results_frame, columns=['type', 'value','confidence'])
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
        #print(add_row)
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
#---------------------------------------------------------------------------------------------------------------------






