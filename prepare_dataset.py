from datetime import datetime

import pandas as pd
import yaml
from databricks.connect import DatabricksSession
from sklearn.model_selection import train_test_split

spark = DatabricksSession.builder.getOrCreate()

with open("project_config.yml", "r") as file:
    config = yaml.safe_load(file)

catalog_name = config.get("catalog_name")
schema_name = config.get("schema_name")

df = pd.read_csv("data/booking.csv")
df["date of reservation"] = (
    df["date of reservation"]
    .apply(lambda x: "3/1/2018" if x == "2018-2-29" else x)
)
df["reservation_date"] = df["date of reservation"].apply(
    lambda x: datetime.strptime(x, "%m/%d/%Y")
)
df["arrival_date"] = df["reservation_date"] + pd.to_timedelta(df["lead time"], unit="d")
df.columns = df.columns.str.replace(" ", "_")
df["booking_status"] = df["booking_status"].replace(
    ["Canceled", "Not_Canceled"], [1, 0]
)
extra_set = df[df["market_segment_type"] != "Online"]
df = df[df["market_segment_type"] == "Online"]

train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)

spark.createDataFrame(extra_set).write.saveAsTable(
    f"{catalog_name}.{schema_name}.extra_set"
)

spark.createDataFrame(train_set).write.saveAsTable(
    f"{catalog_name}.{schema_name}.train_set"
)
spark.createDataFrame(test_set).write.saveAsTable(
    f"{catalog_name}.{schema_name}.test_set"
)

