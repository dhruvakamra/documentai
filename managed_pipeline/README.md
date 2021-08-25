This set of python scripts provide the code for a managed, no-ops and scalabale implementation of DocumentAI

- ingest_document: As the name suggests, uses a GCS bucket to ingest invoices (documents) and triggers the cloud function on commit. The function itself identifies the document, moves it into a temp-processing folder and invokes the DocumentAI API to store a processed JSON blob in return. Once the JSON output is ready this function triggers a pubsub topic ad invokes the process document cloud function.

- process_document: This function takes the JSON blob, normalizes and transforms the data into Panda DFs, creates CSV outputs and uploads the results (using these CSVs) into BQ
