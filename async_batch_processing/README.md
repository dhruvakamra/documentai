This python script provides automation artifacts to:

  1. Make an async-batch processing calls to the Document-AI invoice parser
  2. Process, normalize and transform the output JSON
  3. Create a Pandas dataframe and an output CSV
  4. Upload output CSV to a GCS bucket
  5. Insert the data to BQ tables using the CSVs (uploaded in step (4))

This code set is Google Cloud Functions ready / tested and establishes a no-ops managed invoice processing pipeline viz., 
  1. Invoice upload to GCS input bucket
  2. Cloud function gets trigerred 
  3. Runs through python code as described above

Refer requirements.txt for pre-requisite python packages and libraries 
