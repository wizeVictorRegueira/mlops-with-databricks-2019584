# Databricks notebook source
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    TrafficConfig,
    Route,
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
                Route(served_model_name="basic_model-1",
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
