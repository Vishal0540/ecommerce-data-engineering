from load_data_to_bigquery import dataframe_to_bq_append_mode ,  create_dataset_in_bq
import os 
import pandas as  pd
import sys
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'./bq_credential.json'

# from load_data_to_bigquery import 

if __name__  == '__main__':

    dataset_name = "datamodel" #Bigquery Dataset Name

    create_dataset_in_bq(dataset_name)

    #  iterating over each dataset 

    path_to_reports = 'DataModel'
    for dataset_file in os.listdir(path_to_reports):

        try:
            
            clean_table_name = dataset_file.split(".")[0]

            print(f"Inserting {dataset_file} to Dataset: {dataset_name} Table: {clean_table_name} ")
            
            df = pd.read_csv(os.path.join(path_to_reports,dataset_file))
            df['order_purchase_timestamp'] = df['order_purchase_timestamp'].astype('datetime64[ns]')
            print(df.info())

            dataframe_to_bq_append_mode(df ,f"{dataset_name}.{clean_table_name}")


        except Exception as e:

            exc_type, exc_value, exc_traceback = sys.exc_info()

            error_msg = f"Error in processing file {dataset_file} : error on line {exc_traceback.tb_lineno} \
                        {exc_type}:{exc_value}"

            print(error_msg)
