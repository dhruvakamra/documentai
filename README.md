# DocumentAI Processing
This set of scripts provide automation artifacts in Python to:
1. Make an async (small file) processing calls to the Document-AI invoice parser
2. Process, normalize and transform the output JSON
3. Create a Pandas dataframe and an output CSV
4. Upload output CSV to a GCS bucket 
5. Insert the data to BQ tables using the CSVs (uploaded in step (4))
