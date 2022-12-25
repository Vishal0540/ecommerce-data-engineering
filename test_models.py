import os
import sys
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd

#
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'./bq_credential.json'




def test_order_cutomer_model():


    
    print("Testing Order X Customer Model")
    order_data = pd.read_csv('Data/olist_orders_dataset.csv')

    order_data['order_purchase_timestamp'] = order_data['order_purchase_timestamp'].astype('datetime64[ns]')

    order_data['year'] = order_data['order_purchase_timestamp'].dt.year
    order_data['month'] = order_data['order_purchase_timestamp'].dt.month

    order_data_grouped = order_data.groupby(['year','month'],as_index=False).count()
    # print(order_data_grouped)
    test_data = {}

    for ind , row in order_data_grouped.iterrows():
        test_data[str(row['year'])+"_"+str(row['month'])] = row['order_id']

    print(test_data)

    
    
    bq_client = bigquery.Client()

    query_monthly_orders = '''
    SELECT COUNT(order_id) AS num_orders, EXTRACT(MONTH from order_purchase_timestamp) AS order_month , EXTRACT(YEAR from order_purchase_timestamp) as order_year
FROM `ecommerce-372314.datamodel.OrdersCustomers`
GROUP BY order_month ,order_year 
ORDER BY order_year , order_month 
    '''

    query_job = bq_client.query(query_monthly_orders)


    results = query_job.result()
    # print(results)
    actual_result = {}
    for result in results:
        # print(result)
        actual_result[str(result[2])+"_"+str(result[1])] = result[0]

    print(actual_result)

    
    for k,val in actual_result.items():

        if k in test_data:
            print(f"Test Case for {k} Expected {test_data[k]} , Actual {actual_result[k]}")

            print("Passes" if test_data[k]==test_data[k] else "Fail")
        else:
            print(f"Test Value not available for {k}")
    


def test_order_customer_product():
    

    print("Testing Order X Customer X Product Model")

    avg_amount_by_customer = '''SELECT customer_unique_id , SUM(price)/SUM(order_item_id) as Average_Order_Amount 
FROM `ecommerce-372314.datamodel.OrderCustomerProduct`
WHERE order_status!='canceled' AND DATE(order_purchase_timestamp) BETWEEN '2016-01-01' AND '2020-01-01' 
GROUP BY customer_unique_id
order by customer_unique_id'''


    #Manualy Calculated
    test_case = {
        'de419df8633c2774a7c68912cb578df9':75.0,
        '00172711b30d52eea8b313a7f2cced02':74.5,
        '0097fca0db567f5ca79509b7b4fc1c2d':24.59
    }
    

    bq_client = bigquery.Client()

    query_job = bq_client.query(avg_amount_by_customer)


    results = query_job.result()



    for result in results:
        
        if result[0] in test_case:
            print(f"Testing for customer unique id {result[0]} Expected {test_case[result[0]]} Actual {result[1]}")
    

if __name__ == '__main__':  

    test_order_cutomer_model()

    test_order_customer_product()