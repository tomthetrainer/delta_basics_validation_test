# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Welcome to the Delta Skills Test Notebook
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC The Data Generator Notebook created a table called demo and ran a random collection of operations against it. 
# MAGIC
# MAGIC Your job is to query that table using tools like describe commands and other tools to answer the questions below. 
# MAGIC
# MAGIC Note to Tom, add a timer !! Put now into a variable, and then run command at the end to compare

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Run a setup notebook
# MAGIC
# MAGIC The Setup notebook switches our focus to the DB created by data generator
# MAGIC
# MAGIC Loads a function used for the tes

# COMMAND ----------

# MAGIC %run ./includes/test_code

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Clear the widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Verify Database creation 
# MAGIC
# MAGIC Show tables should show the demo table and the answers table that holds obfuscated answers

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create widgets to enter the answers in

# COMMAND ----------

dbutils.widgets.text(name="01:Number Of Rows", defaultValue="enter row count here")
dbutils.widgets.text(name="02:Number Of Table Versions", defaultValue="enter count here")
dbutils.widgets.text(name="03:What Version was Optimize", defaultValue="enter Optimize Version here")
dbutils.widgets.text(name="04:Zorder Column Name", defaultValue="enter Zorder Column Name here")
dbutils.widgets.text(name="05:TBLProperties Change Version #", defaultValue="enter version # that changed tblproperties")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # The Test
# MAGIC
# MAGIC Please answer the Questions Below

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question 1: Can you count the number of rows in the demo table?
# MAGIC
# MAGIC Enter the value in the Number Of Rows box at the top of the notebook and run the cell below

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Use the first cell to run commands to find your answer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- USE THIS CELL TO write some python or SQL to find the answer
# MAGIC -- When done enter the value in the html widget at the top of the noteboo and run the cell below
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### After entering your answer in the widget, run the cell below to check your answer

# COMMAND ----------

num_rows = dbutils.widgets.get("01:Number Of Rows")
if(python_student_test(1, num_rows)):
    print("Congratulations You have correctly entered the number of rows, proceed to next question")
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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run the cell below to check your answer

# COMMAND ----------

num_versions = dbutils.widgets.get("02:Number Of Table Versions")
if(python_student_test(2, num_versions)):
    print("Congratulations you have succesfully answered the question")
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
# MAGIC -- "What Version was Optimize"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Enter your answer in the widget at the top of the page, and then run the cell below

# COMMAND ----------

num_versions = dbutils.widgets.get("03:What Version was Optimize")
if(python_student_test(3, num_versions)):
    print("Congratulations you have found the correct answer")
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

# MAGIC %sql
# MAGIC -- # Use this cell to find the answer using SQL or python
# MAGIC -- # When you find the answer plact it in the widget title Zorder Column Name
# MAGIC

# COMMAND ----------

num_versions = dbutils.widgets.get("04:Zorder Column Name")
if(python_student_test(4, num_versions)):
    print("Congratulations you have found the correct answer")
else:
    print("Try Again")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Question #5:
# MAGIC
# MAGIC # Setting Delta Table Properties 
# MAGIC
# MAGIC # Which version of the table was created when optimize write was set as a Table property
# MAGIC
# MAGIC
# MAGIC Note that these settings can also be set at the spark session level in addition to being a table property
# MAGIC
# MAGIC The choice was made to set optimizeWrite to true, but to set autoCompact to false.
# MAGIC
# MAGIC Often you would want to set both to true. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Use the cell below to find an answer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your code here
# MAGIC

# COMMAND ----------

num_versions = dbutils.widgets.get("05:TBLProperties Change Version #")
if(python_student_test(5, num_versions)):
    print("Congratulations you have found the correct answer")
else:
    print("Try Again")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Question #6
# MAGIC
# MAGIC Take a look at operationMetrics column from the describe history command.
# MAGIC
# MAGIC In particular note the number of files written in a write operation before optimizWrite was set, and compare it to the number of files written per write operation afteer optimizeWrite was set. 
# MAGIC
# MAGIC Do you see a difference? 
# MAGIC
# MAGIC
# MAGIC Note that no automated test was generated for this difference. 
# MAGIC
# MAGIC This is because expected differences when this content is run on different sized clusters, prevent a guaranteed number in terms of the difference. A 2 node cluster might go from 8 files to 1, while a 4 node cluster might go from 4 files to 1. 
# MAGIC
# MAGIC Also note, that writing multiple files for an input of 10 rows is partially a side effect of the way the data was generated. Typical workloads would not lead to such a serious case of the "small file problem"
