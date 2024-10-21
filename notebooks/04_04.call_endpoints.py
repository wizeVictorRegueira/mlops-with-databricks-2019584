# Databricks notebook source
import os

import requests

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import DataframeSplitInput

workspace = WorkspaceClient()

# COMMAND ----------
workspace.serving_endpoints.query(
    name="hotel-cancellations-preds",
    dataframe_records=[{"Booking_ID": "INN00002"}],
)

# COMMAND ----------
serving_endpoint = f"https://{os.environ['DATABRICKS_HOST']}/serving-endpoints/hotel-cancellations-preds/invocations"
response = requests.post(
    f"{serving_endpoint}",
    headers={"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"},
    json={"dataframe_records": [{"Booking_ID": "INN00002"}]},
)

print("Response status:", response.status_code)
print("Reponse text:", response.text)

# COMMAND ----------

workspace.serving_endpoints.query(
    name="hotel-cancellations-preds",
    dataframe_split=DataframeSplitInput(
        columns=["Booking_ID"],
        data=[["INN00002"]],
    ),
)

# COMMAND ----------
response = requests.post(
    f"{serving_endpoint}",
    headers={"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"},
    json={"dataframe_split": [{
        "columns": ["Booking_ID"],
        "data": [["INN00002"]]}]},
)

print("Response status:", response.status_code)
print("Reponse text:", response.text)

# COMMAND ----------

dataframe_records = [
    {
        "number_of_adults": 1,
        "number_of_children": 0,
        "number_of_weekend_nights": 1,
        "number_of_week_nights": 2,
        "car_parking_space": 1,
        "average_price": 120.5,
        "special_requests": 0,
        "lead_time": 20,
        "repeated": 0,
        "P_C": 0,
        "P_not_C": 1,
        "type_of_meal": "Meal Plan 1",
        "room_type": "Room_Type 1",
    }
]

workspace.serving_endpoints.query(
    name="hotel-cancellations-model",
    dataframe_records=dataframe_records,
)

# COMMAND ----------
model_serving_endpoint = f"https://{os.environ['DATABRICKS_HOST']}/serving-endpoints/hotel-cancellations-model/invocations"
response = requests.post(
    f"{model_serving_endpoint}",
    headers={"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"},
    json={"dataframe_records": dataframe_records},
)

print("Response status:", response.status_code)
print("Reponse text:", response.text)

# COMMAND ----------

workspace.serving_endpoints.query(
    name="hotel-cancellations-model-fe",
    dataframe_records=[
        {
            "Booking_ID": "INN00002",
            "number_of_adults": 1,
            "number_of_children": 0,
            "number_of_weekend_nights": 1,
            "number_of_week_nights": 2,
            "car_parking_space": 1,
            "average_price": 120.5,
            "special_requests": 0,
            "arrival_date": "2024-09-18",
            "reservation_date": "2024-09-08",
            "type_of_meal": "Meal Plan 1",
            "room_type": "Room_Type 1",
        }
    ],
)

# COMMAND ----------
