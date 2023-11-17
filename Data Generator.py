# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # This Notebook Generates the data that the student will be asked some questions about

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Create a database (schema) and use it
# MAGIC
# MAGIC 1. Build a string
# MAGIC 2. Create and use database

# COMMAND ----------

#####
# Extract the username, append a name to it
# And clean out special characters
# Note this may not run as a job, current_user function may not work on jobs
#####
username = spark.sql("select current_user()").collect()[0][0]
#print(username)
database_name = f"{username}_intro_to_stream_monitoring"
#print(database_name)
database_name = (database_name.replace("+", "_").replace("@", "_").replace(".", "_"))
print(database_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create and use Database
# MAGIC
# MAGIC Note this tears down any previous tables in this database

# COMMAND ----------

spark.sql(f"Drop database if exists {database_name} cascade;")
spark.sql(f"Create database if not exists {database_name};")
spark.sql(f"use {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Verify Database creation and create our source table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

from pyspark.sql.types import *
import time
## Define function to create data in a loop
## 10 rows per call
## Schemea (id int, event_type string, timestamp Int)
def create_data(n):
    start = n * 10 ## Set start value for id's
    end = start + 10 ## Set end value for id's
    data = [[i, "logged_in",int(time.time()) ] for i in range(start,end)] # List of Lists
    Schema = StructType([StructField("id", IntegerType()), StructField("event_type", StringType()), StructField("timestamp", IntegerType())]) # Schema
    df = spark.createDataFrame(data,schema=Schema) # Build Dataframe
    return(df)



# COMMAND ----------

spark.sql("create table demo(id int, event_type string, timestamp int)")

# COMMAND ----------

import random

random_number = random.randint(1, 10)
print(random_number)
#for i = 
#random_number = 1

# COMMAND ----------

for x in range(random_number):
  df = create_data(x)
  df.write.mode("append").saveAsTable("demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) from demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from demo

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Next steps
# MAGIC
# MAGIC Write some code that captures the largest version number, 
# MAGIC Wether it has been vacuumed or optimized
# MAGIC The largest value for an id etc. 
# MAGIC
# MAGIC Hash those into a SQL table in the same location and write the tests
