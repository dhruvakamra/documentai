import sys
import os
import google.auth
import re
import base64
import json
import time
from google.cloud import storage
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import pubsub_v1

#---------------------------------------------------------------------------------------------------------------------
######### Initialize variables #########

###------ Project Variables ------###
project_id = "dk-cloudfusion-demo"                                      # Your project-id
### Processor Variables ------###
location = "us"                                                         # Format is 'us' or 'eu'
processorid = "b1b76efccbab279b"                                        # Create processor in Cloud Console
###------ GCS Variables ------###
gcs_input_bucket = 'dk_docai_invoice_drop'
gcs_input_prefix = ''
gcs_output_bucket='dk_docai_temp_bucket'
gcs_output_uri = f'gs://{gcs_output_bucket}'
#gcs_output_uri_prefix = 'docai_temp_processing_bucket'
document_archive_bucket = 'dk_docai_document_archive'
###------ Script Variables ------###
pubsub_topic = 'docai-topic'
subscription_id = 'docai-topic-sub'
###------ Script Variables ------###
TIMEOUT = 120
#index = 0

#---------------------------------------------------------------------------------------------------------------------

def publish_messages(project_id, pubsub_topic, pub_data):
    # Publishes multiple messages to a Pub/Sub topic
    
    print(f"6. Initiating PubSub client ...")
    publisher = pubsub_v1.PublisherClient()
    
    topic_path = publisher.topic_path(project_id, pubsub_topic)
    if not pubsub_topic:
        return ('Missing "topic" and/or "message" parameter.', 400)
    print(f"7. Topic found ... publishing blob name *{pub_data}* to {pubsub_topic} ...")
    
    #Data must be a bytestring
    pub_data = pub_data.encode("utf-8")
    #When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, pub_data)
    print(f"8. Message published as future. Confirmation -->> {future.result()} \n   ...Published")

def ingest_document(event, context):
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))
    ### More sample invoices are stored in gs://cloud-samples-data/documentai/async_invoices/
    ### Moving blob to archive folder
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(gcs_input_bucket, prefix=gcs_input_prefix)
    input_configs = []
    index = 0
    for index, blob in enumerate(blobs):
        source = f"gs://{gcs_input_bucket}/{blob.name}"
        if event['name'] in source:
            print(f"1. Input Files: {source} found in bucket \n   ...Moving  {blob.name} to temp bucket for processing")
            source_bucket = storage_client.bucket(gcs_input_bucket)
            destination_bucket = storage_client.bucket(document_archive_bucket)
            moved_blob = source_bucket.copy_blob(blob, destination_bucket, f'{blob.name}_{time.strftime("%m%d%Y_%H%M%S")}')
            ### delete in old destination
            blob.delete()
            process_sample_document(moved_blob.name, blob.name)
        else:
            index = index + 1

def process_sample_document(blob_name, filename):
    ### Processing blob from archive folder
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(document_archive_bucket)
    input_configs = []
    for blob in blobs:
        if str(blob.name) == str(blob_name):
            source = f'gs://{document_archive_bucket}/{blob.name}'
            print(f"2. Accessing file from new source: {source}")
            
            input_config = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
                gcs_source=source, mime_type="application/pdf")
            input_configs.append(input_config)

    filename = filename.partition('.')
    gcs_output_uri_prefix = 'docai_ingest_temp_output_' + filename[0] + '_' + time.strftime("%m%d%Y-%H%M%S")
    destination_uri = f"{gcs_output_uri}/{gcs_output_uri_prefix}/"
    ### Location to write results
    output_config = documentai.types.document_processor_service.BatchProcessRequest.BatchOutputConfig(
        gcs_destination=destination_uri
    )
    
    ### Instantiates a client
    print(f"3. Initiating DocumentAI client for Invoice Processor *{processorid}* ...")
    client_options = {"api_endpoint": "{}-documentai.googleapis.com".format(location)}
    client = documentai.DocumentProcessorServiceClient(client_options=client_options)
    
    # The full resource name of the processor, e.g.:projects/project_id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first.
    name = f"projects/{project_id}/locations/{location}/processors/{processorid}"
    request = documentai.types.document_processor_service.BatchProcessRequest(
        name=name,
        input_configs=input_configs,
        output_config=output_config,
    )

    print(f"4. Invoking Invoice Processor *{processorid}* and sending {source} for processing ...")
    operation = client.batch_process_documents(request)
    # Wait for the operation to finish
    print("5. Creating JSON output...")
    operation.result(timeout=TIMEOUT)

    # Results are written to GCS. Use a regex to find output files
    match = re.match(r"gs://([^/]+)/(.+)", destination_uri)
    gcs_output_bucket = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(gcs_output_bucket)
    blob_list = list(bucket.list_blobs(prefix=prefix))

    for i, blob in enumerate(blob_list):
    # If JSON file, download the contents of this blob as a bytes object.
        if ".json" in blob.name:
            publish_messages(project_id, pubsub_topic, blob.name)
