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

# MAGIC %md
# MAGIC
# MAGIC # Understanding this notebook
# MAGIC
# MAGIC Creates a table demo and performs the following operations in this order. 
# MAGIC
# MAGIC 1. Create table (version 0)
# MAGIC

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

# MAGIC %md
# MAGIC
# MAGIC Define a function to create data in a loop

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

# MAGIC %md
# MAGIC
# MAGIC # Create Empty Table

# COMMAND ----------

spark.sql("create table demo(id int, event_type string, timestamp int)")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Random number between 1-10 for first batch of insterts

# COMMAND ----------

import random

random_number = random.randint(1, 10)
print(random_number)
#for i = 
#random_number = 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Write the data
# MAGIC
# MAGIC 10 rows per transaction, random number 1-10 transactions
# MAGIC
# MAGIC Data format, 
# MAGIC
# MAGIC ```
# MAGIC 1 logged_in 1700504268
# MAGIC 2 logged_in 1700504268
# MAGIC 3 logged_in 1700504268
# MAGIC ```

# COMMAND ----------

for x in range(random_number):
  df = create_data(x)
  df.write.mode("append").saveAsTable("demo")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Optimize the table

# COMMAND ----------

spark.sql("optimize demo zorder by id")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Add another random number of batch inserts

# COMMAND ----------

random_number = random.randint(1, 10)
## I MAY WANT TO MODIFY THIS SO NUMBERS FOR ID ARE STILL INCREASING
for x in range(random_number):
  df = create_data(x)
  df.write.mode("append").saveAsTable("demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Turn on autoOptimize
# MAGIC
# MAGIC Auto Optimize takes writes that may be about to create many small files and combines into larger files, per that write

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Alter table demo SET TBLPROPERTIES(delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = false)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Add another batch of records to show behavior of optimizeWrite
# MAGIC
# MAGIC Writes before changing the tblproperties may have created one file per slot
# MAGIC
# MAGIC Aftter changing the properties you should see one file per write

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Describe history demo;

# COMMAND ----------

df = create_data(x)
df.write.mode("append").saveAsTable("demo")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # At this point our data has been written
# MAGIC
# MAGIC Next step is to populate an answer table for the student quiz page next notebook

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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create Answer Table
# MAGIC
# MAGIC The answer table with have question ID and hashed answer
# MAGIC
# MAGIC The student will have widgest to answer the question and a validation cell

# COMMAND ----------

spark.sql("create table answers (id int, solution string)")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question 1, can they count rows
# MAGIC
# MAGIC I know simple right, but just getting the process worked out. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Add record to answer table
# MAGIC
# MAGIC 1 is count

# COMMAND ----------

df= spark.sql("select sha2(string(count(*)),256 ) from demo") # get hash of count
value = (df.collect()[0][0])  # Return as local python variable
sql_string = f"insert into answers values(1, '{value}')" # Build a sql string
spark.sql(sql_string) # execute the insert into answers


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # CAN THEY GET LATEST VERSION Number?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question 2: Latest Version Number

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sha2(string(version), 256) from (DESCRIBE HISTORY demo limit 1);

# COMMAND ----------

version_num = spark.sql("SELECT sha2(string(version), 256) from (DESCRIBE HISTORY demo limit 1)").collect()[0][0]
#print(version_num)
sql_string = f"insert into answers values(2, '{version_num}')" # Build a sql string
#print(sql_string)
spark.sql(sql_string) # execute the insert into answers

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question 3: What version was the Optimize?

# COMMAND ----------

# this will break if table has not ever been optimized, I should modify to catch that.
#
version_num = spark.sql("Select sha2(string(version), 256) from (Describe history demo) where operation = 'OPTIMIZE' ").collect()[0][0]
#version_num = spark.sql("SELECT sha2(string(version), 256) from (DESCRIBE HISTORY where operation = 'OPTIMIZE')").collect()[0][0]
#print(version_num)
sql_string = f"insert into answers values(3, '{version_num}')" # Build a sql string
#print(sql_string)
spark.sql(sql_string) # execute the insert into answers

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question 4: What column was used for zorder?
# MAGIC
# MAGIC

# COMMAND ----------

# Hardcode optimize to always be zorder by id, 
# Maybe make that dynamic at some point

version_num = spark.sql("select sha2('id', 256)").collect()[0][0]

#print(version_num)
sql_string = f"insert into answers values(4, '{version_num}')" # Build a sql string
#print(sql_string)
spark.sql(sql_string) # execute the insert into answers


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Describe history demo

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question 5: which version modified the Table Properties?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select version from (describe history demo) where operation = "SET TBLPROPERTIES"

# COMMAND ----------

# Hardcode optimize to always be zorder by id, 
# Maybe make that dynamic at some point

version_num = spark.sql("Select sha2(string(version),256) from (describe history demo) where operation = 'SET TBLPROPERTIES'").collect()[0][0]

#print(version_num)
sql_string = f"insert into answers values(5, '{version_num}')" # Build a sql string
#print(sql_string)
spark.sql(sql_string) # execute the insert into answers


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SOME TESTS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from answers;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe history demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from demo;

# COMMAND ----------

# Keep THis

def python_student_test(question_id, answer):
    if ((spark.sql(f'select sha2("{answer}", 256)')).collect()[0][0]) == spark.sql(f'SELECT solution from answers where id ="{question_id}"').collect()[0][0]:
        return True
    else:
        return False

# COMMAND ----------

python_student_test(2, "8")

# COMMAND ----------

python_student_test(1,"80")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --SET delta.autoOptimize.autoCompact
# MAGIC
# MAGIC -- Turn on auto compact for table
# MAGIC -- something like 
# MAGIC -- Alter table demo SET TBLPROPERTIES(delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
# MAGIC Alter table demo SET TBLPROPERTIES(delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = false)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE history demo;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ADD some records after changing setting
# MAGIC
# MAGIC df = create_data(x)
# MAGIC   df.write.mode("append").saveAsTable("demo")

# COMMAND ----------

df = create_data(x)
df.write.mode("append").saveAsTable("demo")
