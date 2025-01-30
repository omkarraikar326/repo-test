import azure.functions as func
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import BytesIO
from scraper2 import pick_latest_tweet, get_id
import pandas as pd


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


def save_dataframe_to_azure(df, partition_column, container_name):
    print("Initializing save_dataframe_to_azure")

    blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=devnewsweekadls;AccountKey=27yFIzheCgRy+FpbtUAGmfm6tRuhgxyXjRsAiz7VuY4BGNpQcVfbJ7etekwG29dchJsIQ4Mb0jTr+AStYbuHxg==;EndpointSuffix=core.windows.net')
    container_client = blob_service_client.get_container_client(container_name)

    partitions = df[partition_column].unique()
    print(f"Partitions found: {partitions}")

    for partition_value in partitions:
        partition_df = df[df[partition_column] == partition_value]
        
        table = pa.Table.from_pandas(partition_df)
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer, compression='snappy')
        parquet_buffer.seek(0)

        parquet_file_name = "Twitter_downdetector/data.parquet"  
        print(f"Generated Parquet file name: {parquet_file_name}")

        try:
            blob_client = container_client.get_blob_client(parquet_file_name)
            blob_client.upload_blob(parquet_buffer, overwrite=True)
            print(f"Uploaded {parquet_file_name} to Azure Blob Storage")
        except Exception as e:
            print(f"Error uploading {parquet_file_name}: {e}")
            raise
        
        
@app.route(route="http_trigger0",auth_level=func.AuthLevel.FUNCTION)
def http_trigger0(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        user_id = get_id('HPMG')  
        tweet_data_df = pick_latest_tweet(user_id)  

        if not tweet_data_df.empty:
            try:
                save_dataframe_to_azure(tweet_data_df, "created_at", "bronze")
                return func.HttpResponse("Data saved successfully.", status_code=200)
            except Exception as e:
                logging.error(f"Failed to save DataFrame to Azure: {str(e)}")
                return func.HttpResponse(f"Error saving data: {str(e)}", status_code=500)
        else:
            logging.warning("No tweets found in the previous 8 minutes.")
            return func.HttpResponse("No tweets found in the previous 8 minutes.", status_code=200)

    except Exception as e:
        logging.error(f"Error in HTTP trigger function: {str(e)}")
        return func.HttpResponse(f"Internal server error: {str(e)}", status_code=500)