#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=maybe-no-member

# Load libraries
from user_agents import parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import os
import logging
from string import Template
import datetime
import math

# Set Logger
logger = logging.getLogger(__name__)

def query_bigquery(query, location='EU'):
    """Converts a SQL file holding a SQL query to a string.
    Args:
        query: The query argument should be in a string format
    Returns:
        A DataFrame with the results from the query
    """
    try:
        client = bigquery.Client()
        df = client.query(query, location=location.upper()).to_dataframe()
        return df
    except:
        logger.error("Fatal error in query_bigquery function", exc_info=True)

def does_destination_schema_exist(project_id, destination_schema_name, data_location='EU'):
    """Queries BigQuery's information schema to check if the destination schema exist
    Args:
        project_id: The project id of the data and where the parsed results should go to.
        destination_schema_name: The dataset name where the results should go to.
        data_location: The data location of the destination schema
    Returns:
        A Boolean that is true if the schema already exist and false if it does not exist
    """
    try:
        query = """
        SELECT
          CASE 
            WHEN COUNT(*) > 0 THEN TRUE 
            ELSE FALSE 
          END AS does_schema_name_exist
        FROM
          `{project_id}`.INFORMATION_SCHEMA.SCHEMATA
        WHERE
            schema_name = '{schema_name}';
        """.format(project_id = project_id, schema_name=destination_schema_name)
        df_results = query_bigquery(query, data_location)
        does_schema_name_exist = df_results['does_schema_name_exist'].iloc[0]
        return does_schema_name_exist
    except:
        logger.error("Fatal error in does_destination_schema_exist function", exc_info=True)

def does_destination_table_exist(project_id, destination_schema_name, destination_table_name, data_location='EU'):
    """Queries BigQuery's information schema to check if the destination table exist
    Args:
        project_id: The project id of the data and where the parsed results should go to.
        destination_schema_name: The dataset name where the results should go to
        destination_table_name: The table name where the results should go to
        data_location: The data location of the destination table
    Returns:
        A Boolean that is true if the schema already exist and false if it does not exist
    """
    try:
        query = """
        SELECT
          CASE 
            WHEN COUNT(*) > 0 THEN TRUE 
            ELSE FALSE 
          END AS does_table_name_exist
        FROM
          `{project_id}.{schema_name}`.INFORMATION_SCHEMA.TABLES
        WHERE
            table_name = '{table_name}';
        """.format(project_id = project_id, schema_name=destination_schema_name, table_name=destination_table_name)
        df_results = query_bigquery(query, data_location)
        does_table_name_exist = df_results['does_table_name_exist'].iloc[0]
        return does_table_name_exist
    except:
        logger.error("Fatal error in does_destination_table_exist function", exc_info=True)

def get_all_user_agents(project_id, source_schema_name, source_table_name, source_user_agent_field_name, source_timestamp_field, data_location='EU'):
    """ This function gets all user agents from the source table
    Args:
        project_id: The project id of the data and where the parsed results should go to.
        source_schema_name: The dataset name where the source table of user_agents are located
        source_table_name: The table name where the source table of user_agents are located
        source_user_agent_field_name: The name of the user_agent column in the source table
        source_timestamp_field: The timestamp column of when the user_agent was seen
        data_location: The location where the query will be processed
    Returns:
        A dataframe with a user_agent column and a column of the last timestamp the user_agent was seen
    """
    try:
        query = """
        SELECT
            {user_agent_field} AS user_agent,
            MAX({timestamp_field}) AS last_seen_timestamp
        FROM
            `{project_id}.{schema_name}.{table_name}`
        GROUP BY 
            1
        ORDER BY 
            2;
        """.format(
            project_id = project_id, 
            schema_name=source_schema_name, 
            table_name=source_table_name, 
            user_agent_field=source_user_agent_field_name, 
            timestamp_field=source_timestamp_field)
        df_user_agent = query_bigquery(query, data_location)
        return df_user_agent
    except:
        logger.error("Fatal error in does_destination_table_exist function", exc_info=True)

def get_new_user_agents(
    project_id, 
    source_schema_name, 
    source_table_name, 
    source_user_agent_field_name, 
    source_timestamp_field, 
    source_partition_by_field,
    destination_schema_name, 
    destination_table_name, 
    data_location='EU'):
    """ This function gets all user agents from the source table
    Args:
        project_id: The project id of the data and where the parsed results should go to.
        source_schema_name: The dataset name where the source table of user_agents are located
        source_table_name: The table name where the source table of user_agents are located
        source_user_agent_field_name: The name of the user_agent column in the source table
        source_timestamp_field: The timestamp column of when the user_agent was seen
        source_partition_by_field: The name of the field the source table is partitioned by
        destination_schema_name: The dataset name where the results should go to
        destination_table_name: The table name where the results should go to
        data_location: The location where the query will be processed
    Returns:
        A dataframe with a user_agent column and a column of the last timestamp the user_agent was seen
    """
    try:
        query = """
        SELECT
            {source_user_agent_field_name} AS user_agent,
            MAX({source_timestamp_field}) AS last_seen_timestamp
        FROM
            `{project_id}.{source_schema_name}.{source_table_name}` AS source_table
        LEFT JOIN 
            `{project_id}.{destination_schema_name}.{destination_table_name}` destination_table
        ON 
            source_table.{source_user_agent_field_name} = destination_table.user_agent 
        WHERE 
            DATE(source_table.{source_partition_by_field}) >= (SELECT DATE(MAX(last_seen_timestamp)) FROM `{project_id}.{destination_schema_name}.{destination_table_name}`)
            AND destination_table.user_agent IS NULL
        GROUP BY 
            1
        ORDER BY 
            2;
        """.format(
            project_id = project_id, 
            source_schema_name=source_schema_name, 
            source_table_name=source_table_name, 
            source_user_agent_field_name=source_user_agent_field_name, 
            source_timestamp_field=source_timestamp_field,
            source_partition_by_field=source_partition_by_field,
            destination_schema_name=destination_schema_name,
            destination_table_name=destination_table_name
        )
        df_user_agent = query_bigquery(query, data_location)
        return df_user_agent
    except:
        logger.error("Fatal error in does_destination_table_exist function", exc_info=True)

def parse_user_agent(user_agent_string):
    try:
        user_agent_info_dict = {'user_agent': user_agent_string}
        user_agent = parse(user_agent_string)
        user_agent_info_dict['device_browser'] = user_agent.browser.family
        user_agent_info_dict['device_browser_version'] = user_agent.browser.version_string
        user_agent_info_dict['device_os'] = user_agent.os.family
        user_agent_info_dict['device_os_version'] = user_agent.os.version_string
        user_agent_info_dict['device_brand'] = user_agent.device.brand
        user_agent_info_dict['device_model'] = user_agent.device.model
        user_agent_info_dict['device_is_touch_capable'] = user_agent.is_touch_capable
        user_agent_info_dict['is_bot'] = user_agent.is_bot
        if user_agent.is_mobile:
            device_category = 'Mobile'
        elif user_agent.is_tablet:
            device_category = 'Tablet'
        elif user_agent.is_pc:
            device_category = 'Desktop'
        elif user_agent.is_bot:
            device_category = 'Bot'
        else:
            device_category = None
        user_agent_info_dict['device_category'] = device_category
        return user_agent_info_dict
    except:
        logger.error("Fatal in error parse_user_agent function", exc_info=True)

# Function that uploads local file to GCS
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket.
    Args:
        bucket_name: Your Google Cloud Storage bucket name
        source_file_name: path+filename of local file
        destination_blob_name: Name of file in Google Cloud Storage
    Returns: 
        blob_link: The uri of the file that has been uploaded
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)
        blob_link = 'gs://{}/{}'.format(bucket_name, destination_blob_name)
        return blob_link
    except:
        logger.error("Fatal in error upload_blob function", exc_info=True)

# Function that uploads GCS CSV file to BQ
def upload_cloud_storage_csv_file_to_bq_table(blob_link, table_id, insert_method = "WRITE_EMPTY"):
    """Uploads CSV file stored in Google Cloud Storage to BigQuery table.
    Args:
        blob_link: The uri of the file that will be written to BigQuery
        table_id: The table the data is being sent to.
        isert_method: Controls how the data is being inserted. Can either be WRITE_TRUNCATE (overwrite), WRITE_APPEND
        or the default WRITE_EMPTY
    """
    try: 
        # Construct a BigQuery client object.
        client = bigquery.Client()

        if insert_method == "WRITE_TRUNCATE":
            job_config = bigquery.LoadJobConfig(
                autodetect=True, 
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.CSV,
                allow_quoted_newlines = True
            )
        elif insert_method == "WRITE_APPEND":
            job_config = bigquery.LoadJobConfig(
                autodetect=True, 
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.CSV,
                allow_quoted_newlines = True
            )
        else:
            job_config = bigquery.LoadJobConfig(
                autodetect=True, 
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
                source_format=bigquery.SourceFormat.CSV,
                allow_quoted_newlines = True
            )
        load_job = client.load_table_from_uri(blob_link, table_id, job_config=job_config)  
        # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(table_id)
        # print("Loaded {} rows to {}.".format(destination_table.num_rows, table_id))
    except:
        logger.error("Fatal in error upload_cloud_storage_csv_file_to_bq_table function", exc_info=True)

# Function that uploads dataframe to BigQuery
def upload_dataframe_to_bigquery(
    df,
    gcs_bucket,
    csv_file_name,
    table_id,
    local_folder_path = '',
    insert_method = "WRITE_EMPTY"):
    """Uploads dataframe to BigQuery via Google Cloud Storage using either truncate, append or write of empty method.
    Args:
        - df: A dataframe you want to upload to BigQuery. Note that the schema has to be the same as the destiantion BigQuery
        table if you choose to append data.
        - gcs_buckets: Google Cloud Storage bucket name that the CSV file the csv file will be uploaded to.
        - csv_file_name: The name of the csv file that will be created for GCS and the temporary csv file, that will be created.
        - table_id: The table Id for the BigQuery table the data will be inserted into using your defined method.
        - local_folder_path: The path to the folder where the temporary csv file should be saved. Set to your current path
        by defualt.
        - isert_method: Controls how the data is being inserted. Can either be WRITE_TRUNCATE (overwrite), WRITE_APPEND
        or the default WRITE_EMPTY.
    Returns:
    """
    try:
        # Check if csv_file_name contains .csv else add it to the name
        if '.csv' not in csv_file_name:
            csv_file_name = csv_file_name + '.csv'
        # Save local file
        csv_file_path = local_folder_path+csv_file_name
        df.to_csv(csv_file_path, encoding="utf-8", index=False)
        #Upload local CSV file to GCS
        blob_link = upload_blob(bucket_name=gcs_bucket,
                                source_file_name = csv_file_path,
                                destination_blob_name = csv_file_name)
        # Upload CSV file from GCS to BQ table using either truncate, append or write of empty method.
        upload_cloud_storage_csv_file_to_bq_table(blob_link, table_id, insert_method)
        # Delete local csv file
        os.remove(csv_file_path)
    except:
        logger.error("Fatal in error upload_new_predictions_to_bigquery function", exc_info=True)

def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            # Set variables based on enviroment variables. Define these in the Cloud Function settings
            project_id = os.environ.get('project_id')
            data_location = os.environ.get('data_location')
            source_schema_name = os.environ.get('source_schema_name')
            source_table_name = os.environ.get('source_table_name')
            source_partition_by_field = os.environ.get('source_partition_by_field')
            source_timestamp_field = os.environ.get('source_timestamp_field')
            source_user_agent_field_name = os.environ.get('source_user_agent_field_name')
            destination_schema_name = os.environ.get('destination_schema_name')
            destination_table_name = os.environ.get('destination_table_name')
            gcs_bucket_name = os.environ.get('gcs_bucket_name')
            local_folder_path = os.environ.get('local_folder_path')
            # Logic to check if destination dataset exist. If it does not exist we create the dataset
            does_destination_schema_exist_result = does_destination_schema_exist(project_id, destination_schema_name)
            if does_destination_schema_exist_result == False:
                client = bigquery.Client(project_id)
                dataset = bigquery.Dataset(project_id+'.'+destination_schema_name)
                dataset.location = data_location.upper()
                dataset = client.create_dataset(dataset, timeout=30)
                print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
            # Check if the destination table exist. If it exist we do an incremental run else we do a full refresh
            does_destination_table_exist_result = does_destination_table_exist(project_id, destination_schema_name, destination_table_name, data_location)
            if does_destination_table_exist_result == True:
                print('Incremental refresh')
                df_user_agent = get_new_user_agents(
                    project_id, 
                    source_schema_name, 
                    source_table_name, 
                    source_user_agent_field_name, 
                    source_timestamp_field, 
                    source_partition_by_field,
                    destination_schema_name, 
                    destination_table_name, 
                    data_location)
            else:
                print('Full refresh')
                df_user_agent = get_all_user_agents(
                    project_id, 
                    source_schema_name, 
                    source_table_name, 
                    source_user_agent_field_name, 
                    source_timestamp_field, 
                    data_location)
            # Set variables for parsing loop
            legnth_of_dataframe = len(df_user_agent)
            batch_size = 10000
            number_of_batches = math.ceil(legnth_of_dataframe/batch_size)
            start_row = 0
            end_row = batch_size
            # Check if there are any new user_agents
            if legnth_of_dataframe > 0:
                # We split the dataframe up into batches to make it possible to see progress and save parsed user_agents as we go along
                for i in range(number_of_batches):
                    print('Starting batch number {batch_number} out of {number_of_batches}'.format(batch_number=i+1, number_of_batches=number_of_batches))
                    df_user_agent_batch = df_user_agent.iloc[start_row:end_row]
                    start_row = start_row+batch_size
                    end_row = end_row+batch_size
                    # Define list to append results to
                    list_of_parsed_results = []
                    # Define pool of works for parallel excecution
                    with ThreadPoolExecutor() as executor:
                        threads= []
                        # Loop through unknown user agents df_user_agent['user_agent']
                        for user_agent in df_user_agent_batch['user_agent']:
                            threads.append(executor.submit(parse_user_agent, user_agent))
                        # Append results from workers to list of results
                        for thread in as_completed(threads):
                            list_of_parsed_results.append(thread.result())
                        # Convert list to a Dataframe
                        df_parsed_user_agent_results_batch = pd.DataFrame(list_of_parsed_results)

                        
                        # Enforce correct data types. All columns except is_touch_capable should be strings
                        cols=[i for i in df_parsed_user_agent_results_batch.columns if i not in ("device_is_touch_capable", "is_bot")]
                        for col in cols:
                            df_parsed_user_agent_results_batch[col] = df_parsed_user_agent_results_batch[col].astype(str)
                            # Fill blanks and None values
                            df_parsed_user_agent_results_batch.fillna(value='Unknown', inplace=True)
                            df_parsed_user_agent_results_batch = df_parsed_user_agent_results_batch.replace(r'^\s*$', 'Unknown', regex=True)
                        # Make sure boolean fields are of type boolean
                        df_parsed_user_agent_results_batch['device_is_touch_capable'] = df_parsed_user_agent_results_batch['device_is_touch_capable'].astype(bool)
                        df_parsed_user_agent_results_batch['is_bot'] = df_parsed_user_agent_results_batch['is_bot'].astype(bool)
                        df_parsed_user_agent_batch_final = pd.merge(df_parsed_user_agent_results_batch, df_user_agent_batch, on='user_agent')
                        # Upload new user agents to BigQuery
                        upload_dataframe_to_bigquery(df = df_parsed_user_agent_batch_final,
                                                    gcs_bucket = gcs_bucket_name,
                                                    csv_file_name = 'new_user_agents_batch_{}.csv'.format(i+1),
                                                    table_id = destination_schema_name+'.'+destination_table_name,
                                                    local_folder_path = local_folder_path,
                                                    insert_method = "WRITE_APPEND")
                        print("Loaded {number_of_rows} rows to {schema_name}.{table_name}".format(
                            number_of_rows=str(len(df_parsed_user_agent_batch_final)), 
                            schema_name=destination_schema_name, 
                            table_name=destination_table_name)
                             )                  
            else:
                print('No new user agents to parse')
            

        except Exception as error:
            log_message = Template('Script failed due to '
                                   '$message.')
            logging.error(log_message.safe_substitute(message=error))

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)
