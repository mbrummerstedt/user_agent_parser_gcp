# Parse new user_agents in BigQuery 

## The purpose of the function:
Segment collects the user-agent for client-side events, which contains information about the device that visits the site. This string holds little information to humans in its raw form. 
We, therefore, need to parse the raw user-agents to extract understandable information from them. 

Python has a good library for this, which is why we use a python cloud-function for the job

## Steps in Function:

1. Check if the destination dataset exist. If not create it.
2. Check if the destination table exist. If it exist we load only new user agents that has not been parsed before. Else we load all user_agents
3. Split large number of user agents into batches for eaier debugging.
2. Use the user-agent parser library to parse each new user-agent
3. Enforce the correct data types for each column
4. Load results into the user_agent_lookup table in the marketing project


## How to setup

1. Have a table with user_agents and a timestamp column in BigQuery
2. Create a Google Cloud Storage Bucket that can be used for staging files
3. Create a pub/sub topic 
4. Create a Cloud Schedueler that sends messages to your pub/sub topic
5. Create a Google Cloud Function with the code that get's triggered by your pub/sub topic. 

Add the following runtime variables to your cloud function: 

- **project_id:** The id of of your GCP project
- **data_location:** The location of your BigQuery data (e.g. EU or US)
- **source_schema_name:** The name of your dataset where the user_agent data is stored
- **source_table_name:** The table name  where the user_agent data is stored
- **source_partition_by_field:** The partition by field of the source table. If it is not partitioned just pass a timestamp column.
- **source_timestamp_field:** The timestamp column name of the primary timestamp of your source table. 
- **source_user_agent_field_name:** The user_agent column name of your source table.
- **destination_schema_name:** The name of your destination dataset.
- **destination_table_name:** The name of your destination table where the results will be loaded into
- **gcs_bucket_name:** The name of your Google Cloud Storage bucket
- **local_folder_path:** The folder path where the script can write a temporary CSV file to. This should be /tmp/ for Cloud functions
