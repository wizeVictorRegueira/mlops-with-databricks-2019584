# Databricks notebook source
# MAGIC %pip install lightgbm===4.5.0 scikit-learn==1.5.1 cloudpickle==3.0.0 mlflow==2.16.0 pandas==2.2.2 databricks-feature-engineering==0.6 databricks-sdk==0.32.0

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

from pyspark.sql import SparkSession
from databricks.feature_engineering import FeatureLookup
import mlflow
import yaml
from databricks import feature_engineering
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    OnlineTableSpec,
    OnlineTableSpecTriggeredSchedulingPolicy,
)
import pandas as pd

# COMMAND ----------

workspace = WorkspaceClient()
fe = feature_engineering.FeatureEngineeringClient()

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

with open("../project_config.yml", "r") as file:
    config = yaml.safe_load(file)

num_features = config.get("num_features")
cat_features = config.get("cat_features")
target = config.get("target")
catalog_name = config.get("catalog_name")
schema_name = config.get("schema_name")

feature_table_name = f"{catalog_name}.{schema_name}.cancellation_preds"
online_table_name = f"{catalog_name}.{schema_name}.cancellation_preds_online"

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

train_set = spark.table(f"{catalog_name}.{schema_name}.train_set").toPandas()
test_set = spark.table(f"{catalog_name}.{schema_name}.test_set").toPandas()

df = pd.concat([train_set, test_set])

# COMMAND ----------

run_id = mlflow.search_runs(
    experiment_names=["/Shared/hotel-cancellations-basic"],
    filter_string="tags.branch='02_04'",
).run_id[0]
pipeline = mlflow.sklearn.load_model(
  f'runs:/{run_id}/lightgbm-pipeline-model')

preds_df = df[['Booking_ID']]
preds_df['Cancellation_prob'] = [x[1] for x in pipeline.predict_proba(
  df[cat_features+num_features])]
preds_df['Cancelation_preds'] = ['Cancelled' if x==1  else 'Not cancelled' 
                                 for x in pipeline.predict(
                                   df[cat_features+num_features])]
preds_df = spark.createDataFrame(preds_df)

# COMMAND ----------

fe.create_table(
  name = feature_table_name,
  primary_keys=["Booking_ID"],
  df = preds_df,
  description = "Hotel booking cancellation predictions"
)


# COMMAND ----------

spark.sql(f"ALTER TABLE {feature_table_name} "
          "SET TBLPROPERTIES (delta.enableChangeDataFeed = true);")

# COMMAND ----------

spec = OnlineTableSpec(
    primary_key_columns=["Booking_ID"],
    source_table_full_name=f"{feature_table_name}",
    run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict(
        {"triggered": "true"}
    ),
    perform_full_copy=False,
)

online_table_pipeline = workspace.online_tables.create(
    name=online_table_name, spec=spec
)

# COMMAND ----------

online_table_pipeline

# COMMAND ----------

features=[
  FeatureLookup(
    table_name=feature_table_name,
    lookup_key="Booking_ID"
  )
]
feature_spec_name = f"{catalog_name}.{schema_name}.return_predictions"

fe.create_feature_spec(name=feature_spec_name, features=features, exclude_columns=None)
