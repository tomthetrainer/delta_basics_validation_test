# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Please answer the following questions regarding the demo table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Create a database (schema) and use it
# MAGIC
# MAGIC 1. Build a string
# MAGIC 2. Create and use database

# COMMAND ----------

dbutils.widgets.removeAll()

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

spark.sql(f"use {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Verify Database creation 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question 1: Count the rows in the demo table
# MAGIC
# MAGIC I know simple right, but this is still a useful test to make sure the code is functioning

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Run a notebook that creates a python function used to validate your responses

# COMMAND ----------

# MAGIC %run ./test_code

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Add a widgets for your answers

# COMMAND ----------

dbutils.widgets.text(name="Number Of Rows", defaultValue="enter row count here")

# COMMAND ----------

dbutils.widgets.text(name="Number Of Table Versions", defaultValue="enter count here")

# COMMAND ----------

dbutils.widgets.text(name="What Version was Optimize", defaultValue="enter Optimize Version here")

# COMMAND ----------

dbutils.widgets.text(name="Zorder Column Name", defaultValue="enter Zorder Column Name here")

# COMMAND ----------

dbutils.widgets.text(name="TBLProperties Change Version #", defaultValue="enter version # that changed tblproperties")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question 1: Can you count the number of rows in the demo table?
# MAGIC
# MAGIC Enter the value in the Number Of Rows box at the top of the notebook and run the cell below

# COMMAND ----------

# MAGIC %sql
# MAGIC -- USE THIS CELL TO write some python or SQL to find the answer
# MAGIC -- When done enter the value in the html widget at the top of the noteboo and run the cell below
# MAGIC SELECT count(*) from demo
# MAGIC

# COMMAND ----------

num_rows = dbutils.widgets.get("Number Of Rows")
if(python_student_test(1, num_rows)):
    print("Congratulations")
else:
    print("Try Again")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question Number 2, what is the latest version number of the demo table?
# MAGIC
# MAGIC Enter your value in Number Of Table Versions at top of page

# COMMAND ----------

# MAGIC %sql
# MAGIC -- USE THIS CELL TO write some python or SQL to find the answer
# MAGIC -- When done enter the value in the html widget at the top of the noteboo and run the cell below
# MAGIC Describe history demo

# COMMAND ----------

num_versions = dbutils.widgets.get("Number Of Table Versions")
if(python_student_test(2, num_versions)):
    print("Congratulations")
else:
    print("Try Again")

# COMMAND ----------

# MAGIC %md
# MAGIC # Question Number 3
# MAGIC # Has the table been optimized?
# MAGIC
# MAGIC If the table has been optimized, please enter the version of the table that ran the optimize operation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use this cell to write some SQL or Python
# MAGIC -- to find the answer
# MAGIC -- Then answer by entering the version number in the widget
# MAGIC -- "Whate Version was Optimize"
# MAGIC DESCRIBE HISTORY demo;

# COMMAND ----------

num_versions = dbutils.widgets.get("What Version was Optimize")
if(python_student_test(3, num_versions)):
    print("Congratulations")
else:
    print("Try Again")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question # 4
# MAGIC
# MAGIC # If the table has been optimized, was a Zorder column set?
# MAGIC
# MAGIC # If so what column has the table been zordered by?

# COMMAND ----------

# Use this cell to find the answer using SQL or python
# When you find the answer plact it in the widget title Zorder Column Name

# COMMAND ----------

num_versions = dbutils.widgets.get("Zorder Column Name")
if(python_student_test(4, num_versions)):
    print("Congratulations")
else:
    print("Try Again")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question #5:
# MAGIC
# MAGIC # Delta a few settings that can be set either at the spark session level or per table. 
# MAGIC
# MAGIC Optimize write and auto compact are the settings. 
# MAGIC
# MAGIC Note that these may be the default for merge and update operations. 
# MAGIC
# MAGIC It may be useful to set them for appends. In this example, due to the way we create the dataframe, we are most likely writing one file per task slot per worker. This would be 8 files for 10 rows when ran on a two node cluster. It would be 4 files for 10 rows on a single node cluster. Although optimize is useful for managing a small file problem, optimize write is useful per write. 
# MAGIC
# MAGIC # Which version of the table was created when optimize write was set as a Table property

# COMMAND ----------

num_versions = dbutils.widgets.get("TBLProperties Change Version #")
if(python_student_test(5, num_versions)):
    print("Congratulations")
else:
    print("Try Again")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from answers;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo;
