# Databricks notebook source
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession
import yaml
import time

workspace = WorkspaceClient()

with open("../project_config.yml", "r") as file:
    config = yaml.safe_load(file)

catalog_name = config.get("catalog_name")
schema_name = config.get("schema_name")

spark = DatabricksSession.builder.getOrCreate()

test_set = spark.table(f"{catalog_name}.{schema_name}.test_set").toPandas()
extra_set = spark.table(f"{catalog_name}.{schema_name}.extra_set").toPandas()
df_test = test_set
df_extra = extra_set[extra_set['market_segment_type']=='Offline']

# COMMAND ----------
def send_request(row):
    workspace.serving_endpoints.query(
        name="hotel-cancellations-model-fe",
        dataframe_records=[{
            "Booking_ID": row["Booking_ID"],
            "number_of_adults": row["number_of_adults"],
            "number_of_children": row["number_of_children"],
            "number_of_weekend_nights": row["number_of_weekend_nights"],
            "number_of_week_nights": row["number_of_week_nights"],
            "car_parking_space": row["car_parking_space"],
            "average_price": row["average_price"],
            "special_requests": row["special_requests"],
            "arrival_date": row["arrival_date"].strftime("%Y-%m-%d %H:%M:%S"),
            "reservation_date": row["reservation_date"].strftime("%Y-%m-%d %H:%M:%S"),
            "type_of_meal": row["type_of_meal"],
            "room_type": row["room_type"]
        }]
    )

# Loop over rows and send requests
for index, row in df_test.iterrows():
    print(f"sending test df request for index {index}")
    send_request(row)
    time.sleep(0.5) 
    
for index, row in df_extra.iterrows():
    print(f"sending extra df request for index {index}")
    send_request(row)
    time.sleep(0.5) 
# COMMAND ----------
