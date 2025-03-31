# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "550afeaa-8558-4439-beb3-a3ad9c9984d9",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "3c287734-4f67-40e9-8c8b-bcc3d93ff862",
# META       "known_lakehouses": [
# META         {
# META           "id": "550afeaa-8558-4439-beb3-a3ad9c9984d9"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

wwiRootFolder = "Files/WideWorldImportersDW"
factSaleFolder = "facts/fact_sale_1y_incremental"

environment = "~interactive~"
version = "0.0.0"
pipeline = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Spark session configuration
# This cell sets Spark session settings to enable _Verti-Parquet_ and _Optimize on Write_. More details about _Verti-Parquet_ and _Optimize on Write_ in tutorial document.

# CELL ********************

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact - Sales
# 
# This cell reads raw data from the _Files_ section of the lakehouse, adds additional columns for different date parts and the same information is being used to create partitioned fact delta table.

# CELL ********************

from pyspark.sql.functions import col, year, month, quarter

table_name = 'Sales'

df = spark.read.format("parquet").load(f"{wwiRootFolder}/{factSaleFolder}") \
  .select("SaleKey", "CityKey", "CustomerKey", "SalespersonKey", "StockItemKey", "InvoiceDateKey",
    "Package", "Quantity", "TotalDryItems", "TotalChillerItems",
    "UnitPrice", "TaxAmount", "Profit"
  ) \
  .withColumn('Year', year(col("InvoiceDateKey"))) \
  .withColumn('Quarter', quarter(col("InvoiceDateKey"))) \
  .withColumn('Month', month(col("InvoiceDateKey")))

df.write.mode("overwrite").format("delta").partitionBy("Year","Quarter").save(f"Tables/{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dimensions
# This cell creates a function to read raw data from the _Files_ section of the lakehouse for the table name passed as a parameter. Next, it creates a list of dimension tables. Finally, it has a _for loop_ to loop through the list of tables and call above function with each table name as parameter to read data for that specific table and create delta table.

# CELL ********************

from pyspark.sql.types import *

def loadFullDataFromSource(sourceFolder, tableInfo):
    df = spark.read.format("parquet").load(f"{wwiRootFolder}/{sourceFolder}")
    df = df.select(tableInfo["columns"])
    df.write.mode("overwrite").format("delta").save("Tables/" + tableInfo["name"])

full_tables = [
    'dimension_city',
    'dimension_customer',
    'dimension_date',
    'dimension_employee',
    'dimension_stock_item'
]

tables = [
    ("dimension_city", { "name": "City", "columns": [ "CityKey", "City", "Country", "Continent", "StateProvince", "SalesTerritory", "Region", "Subregion" ]}),
    ("dimension_customer", { "name": "Customer", "columns": ["CustomerKey","Customer","Category","BuyingGroup", "PostalCode"]}),
    ("dimension_employee", { "name": "Employee", "columns": ["EmployeeKey","Employee","IsSalesperson","PreferredName"]}),
    ("dimension_stock_item", { "name": "Stock_Item", "columns": ["StockItemKey","StockItem","Color","Brand","Size","IsChillerStock"]}),
    ("dimension_date", { "name": "Date", "columns": ["Date", "CalendarMonthNumber", "FiscalMonthNumber",
        "Month", "ShortMonth",
        col("CalendarMonthLabel").alias("CalendarMonth"),
        col("CalendarYearLabel").alias("CalendarYear"),
        col("FiscalMonthLabel").alias("FiscalMonth"),
        col("FiscalYearLabel").alias("FiscalYear"),
    ]}),
]

for sourceFolder,tableInfo in tables:
    loadFullDataFromSource(sourceFolder,tableInfo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Info Table

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [
    ("Environment", environment),
    ("Version", version),
    ("Pipeline", pipeline)
]
schema = StructType([
    StructField("key", StringType(), False),
    StructField("value", StringType(), True)
])
df = spark.createDataFrame(data, schema)
df.show()

df.write.mode("overwrite").format("delta").save("Tables/Info")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
