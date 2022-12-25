import os
import sys
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd

#
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'./bq_credential.json'



def dataframe_to_bq_append_mode(df,table_name):

    '''
    Laods data from pandas dataframe to Bigguery table in WRITE_APPEND mode

    Args:
        table_name (str): Destination table name with datset prefix
        df (pandas.DataFrame): The second number to add.
    
    Returns:
        None
    '''
    try:
        client_bq = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            schema_update_options='ALLOW_FIELD_ADDITION'
        )
        load_job = client_bq.load_table_from_dataframe(df,table_name,job_config)
        load_job.result()
    except Exception as e:

        exc_type, exc_value, exc_traceback = sys.exc_info()

        error_msg = f"Error in loading data to {table_name} : error on line {exc_traceback.tb_lineno} \
                    {exc_type}:{exc_value}"

        print(error_msg)



def create_dataset_in_bq(dataset_name):

    '''
        creates dataset if not exists
        Args:
            datset_name (int): name of datset
    
        Returns:
            None
    '''
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_name)
    try:
        bigquery_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created'.format(dataset.dataset_id))







if __name__  == '__main__':

    dataset_name = "ecommerce" #Bigquery Dataset Name

    create_dataset_in_bq(dataset_name)

    #  iterating over each dataset 
    for dataset_file in os.listdir('Data'):

        try:
            table_name = dataset_file.split(".")[0].title()
            clean_table_name = table_name.replace("_","")

            print(f"Inserting {dataset_file} to Dataset: {dataset_name} Table: {clean_table_name} ")
            
            df = pd.read_csv("Data/"+dataset_file,dtype=str)

            dataframe_to_bq_append_mode(df ,f"{dataset_name}.{clean_table_name}")


        except Exception as e:

            exc_type, exc_value, exc_traceback = sys.exc_info()

            error_msg = f"Error in processing file {dataset_file} : error on line {exc_traceback.tb_lineno} \
                        {exc_type}:{exc_value}"

            print(error_msg)





    