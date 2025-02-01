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

expected_schema = StructType(
    [
        StructField('id',IntegerType(),False),
        StructField('date',DateType(),True),
        StructField('deadline_date',DateType(),True),
        StructField('title',StringType(),True),
        StructField('category',StringType(),True),
        StructField('phase',StringType(),True),
        StructField('awarded_value_in_euros',LongType(),True)
    ]
)

# Data Validation - Schema Validation (Number of columns check)
def col_count_check(expected_schema,extracted_schema):
    missing_columns = [col.name for col in expected_schema if col.name not in extracted_schema]
    extra_columns = [col for col in extracted_schema if col not in [col.name for col in expected_schema]]

    if len(missing_columns) > 0:
         print(f"You have some missing columns!! - {missing_columns}")
    elif len(extra_columns) > 0:
        print(f"You have received some extra columns !!! - {extra_columns} ")
    else:
        print("All the required columns are extracted correctly")
    
   

col_count_check(expected_schema,final_df.columns)


# Casting different datatype to different columns

for x in expected_schema.fields:
    dtype = x.dataType
    col_name = x.name
    final_df = final_df.withColumn(col_name, final_df[col_name].cast(dtype))

    # Replacing null values with default value

# replacing null values in deadline date column
final_df = final_df.withColumn('deadline_date', coalesce(final_df['deadline_date'], lit('9999-99-9')))

# replacing null vlaues in other columns
final_df = final_df.na.fill('unknown',['phase'])\
    .na.fill(value = 0, subset=['awarded_value_in_euros'])

# counting null values in different columns of dataframe

null_count_df = final_df.select([count(when(col(c).isNull(),1)).alias(c) for c in final_df.columns])
null_count_df.show()