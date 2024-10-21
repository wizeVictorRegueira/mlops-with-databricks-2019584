# Databricks notebook source
# MAGIC %pip install lightgbm===4.5.0 scikit-learn==1.5.1 cloudpickle==3.0.0 mlflow==2.16.0 pandas==2.2.2 databricks-feature-engineering==0.6 databricks-sdk==0.32.0

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    TrafficConfig,
    Route,
)
from databricks.sdk.service.catalog import (
    OnlineTableSpec,
    OnlineTableSpecTriggeredSchedulingPolicy,
)

workspace = WorkspaceClient()

with open("../project_config.yml", "r") as file:
    config = yaml.safe_load(file)

catalog_name = config.get("catalog_name")
schema_name = config.get("schema_name")

# COMMAND ----------

workspace.serving_endpoints.create(
    name="hotel-cancellations-model",
    config=EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name=f"{catalog_name}.{schema_name}.basic_model",
                scale_to_zero_enabled=True,
                workload_size="Small",
                entity_version=1,
            )
        ],
        # Optional if only 1 entity is served
        traffic_config=TrafficConfig(
            routes=[
                Route(served_model_name=f"basic_model-1",
                      traffic_percentage=100)
            ]
        ),
    ),
)


# COMMAND ----------

workspace.serving_endpoints.create(
    name="hotel-cancellations-preds",
    config=EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name=f"{catalog_name}.{schema_name}.return_predictions",
                scale_to_zero_enabled=True,
                workload_size="Small",
            )
        ]
    ),
)

# COMMAND ----------

feature_table_name = f"{catalog_name}.{schema_name}.hotel_features"
online_table_name = f"{catalog_name}.{schema_name}.hotel_features_online"

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

workspace.serving_endpoints.create(
    name="hotel-cancellations-model-fe",
    config=EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name=f"{catalog_name}.{schema_name}.hotel_booking_model_fe",
                scale_to_zero_enabled=True,
                workload_size="Small",
                entity_version=1,
            )
        ]
    ),
)
