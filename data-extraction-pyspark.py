import json
import requests
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sc = SparkContext()
spark = SparkSession(sc)

def get_tender_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        all_tenders = []
        data = response.json()
        tender_list = data['data']
        for tender in tender_list:
            tender_details = {}
            tender_details['id'] = tender.get('id',None)
            tender_details['date'] = tender.get('date',None)
            tender_details['deadline_date'] = tender.get('deadline_date', None)
            tender_details['title'] = tender.get('title', None)
            tender_details['category'] = tender.get('category', None)
            tender_details['phase'] = tender.get('phase_en', None)
            tender_details['awarded_value_in_euros'] = tender.get('awarded_value_eur', None)
            all_tenders.append(tender_details)


        return all_tenders
    else:
        return None

# getting data for procurements in Hungary, Poland, Romania, Spain, Ukraine
url_list = ["https://tenders.guru/api/hu/tenders","https://tenders.guru/api/pl/tenders","https://tenders.guru/api/ro/tenders","https://tenders.guru/api/es/tenders","https://tenders.guru/api/ua/tenders"]
merged_data_list = []
# merged_data_df = pd.DataFrame()
for url in url_list:
    data = get_tender_data(url)
    # df = pd.DataFrame(data)
    merged_data_list.append(data) 


df_list = []
for data in merged_data_list:
    # json.dumps(data) will convert python objects like lists or dictionary to JSON string
    # [json.dumps(data)] will create a list of json strings
    # sc.parallelize([json.dumps(data)]) will distribute the python list objects across worker nodes there by creating a RDD
    # spark.read.json will create a data frame from RDD
    df = spark.read.json(sc.parallelize([json.dumps(data)]))
    df_list.append(df)

final_df = df_list[0]
for df in df_list[1:]:
    final_df = final_df.union(df)
