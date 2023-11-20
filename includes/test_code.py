# Databricks notebook source
# This is the test function
# Table has sha2(256) hash of correct answer
# Student provides answer
# This code compares the two




def python_student_test(question_id, answer):
    if ((spark.sql(f'select sha2("{answer}", 256)')).collect()[0][0]) == spark.sql(f'SELECT solution from answers where id ="{question_id}"').collect()[0][0]:
        return True
    else:
        return False

# COMMAND ----------

#####
# Extract the username, append a name to it
# And clean out special characters
# Note this may not run as a job, current_user function may not work on jobs
#####
username = spark.sql("select current_user()").collect()[0][0]
#print(username)
database_name = f"{username}_basic_delta_test"
#print(database_name)
database_name = (database_name.replace("+", "_").replace("@", "_").replace(".", "_"))
#print(database_name)
spark.sql(f"use {database_name}")

# COMMAND ----------


