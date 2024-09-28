# Databricks notebook source
import yaml
from databricks import feature_engineering
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession

workspace = WorkspaceClient()
fe = feature_engineering.FeatureEngineeringClient()

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------
with open("../project_config.yml", "r") as file:
    config = yaml.safe_load(file)

catalog_name = config.get("catalog_name")
schema_name = config.get("schema_name")

# COMMAND ----------
feature_table_name = f"{catalog_name}.{schema_name}.hotel_features"
function_name = f"{catalog_name}.{schema_name}.calculate_lead_time"

train_set = spark.table(f"{catalog_name}.{schema_name}.train_set")
test_set = spark.table(f"{catalog_name}.{schema_name}.test_set")

hotel_features_df = train_set[["Booking_ID", "repeated", "P-C", "P-not-C"]]
train_set = train_set.drop("lead_time", "repeated", "P-C", "P-not-C")

# COMMAND ----------
feature_table = fe.create_table(
    name=feature_table_name,
    primary_keys=["Booking_ID"],
    df=hotel_features_df,
    description="Hotel booking features",
)

# COMMAND ----------
fe.write_table(
    name=feature_table_name,
    df=test_set[["Booking_ID", "repeated", "P-C", "P-not-C"]],
    mode="merge",
)

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE FUNCTION {function_name}(arrival_date TIMESTAMP, reservation_date TIMESTAMP)
RETURNS INT
LANGUAGE PYTHON AS
$$
return (arrival_date-reservation_date).days
$$""")