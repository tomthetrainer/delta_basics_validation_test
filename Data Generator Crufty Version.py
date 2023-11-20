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
# MAGIC describe history demo limit 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) from demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES;

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
# MAGIC # CAN THEY GET LATEST VERSION Number

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

# MAGIC %sql
# MAGIC SELECT * FROM answers;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SOME TESTS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) from demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe history demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (SELECT solution from answers where id=1) = (SELECT sha2('50', 256));

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION student_test( question_id int, answer STRING)
# MAGIC   
# MAGIC --RETURNS BOOLEAN
# MAGIC RETURNS STRING
# MAGIC RETURN SELECT solution from answers where id=question_id;
# MAGIC --RETURN SELECT '${answers.1}'== sha2(answer,256 ) as id
# MAGIC --RETURN SELECT '${concat("answers.",question_id)}'
# MAGIC --RETURN SELECT '${concat("answers.",question_id)}'== sha2(answer,256 ) as id
# MAGIC --RETURN SELECT '${answers.1}'
# MAGIC --RETURN (SELECT sha2(answer, 256)) = (SELECT solution from answers where id=1 limit 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from answers;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP FUNCTION stests

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION stests(question_id INT, answer STRING) 
# MAGIC    RETURNS STRING
# MAGIC    READS SQL DATA SQL SECURITY DEFINER
# MAGIC    COMMENT 'Translates an RGB color code into a color name'
# MAGIC    --RETURN "HEY"
# MAGIC    --RETURN SELECT solution from answers where id=stests.question_id limit 1;
# MAGIC   RETURN SELECT solution from answers where id=question_id;
# MAGIC   --RETURN stests.question_id
# MAGIC   -- RETURN SELECT stests.question_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT solution from answers where id =1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT stests(8,"hey")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Let's push answers into spark vars

# COMMAND ----------

# NOT THIS ONE
df = spark.sql("select * from answers") # Define DF
answer_list = df.collect()
for row in answer_list: 
    print(row.id)
    row_num = row.id
    solution = row.solution
    print(row.solution)
    spark.conf.set(f"answers.{row_num}",solution)


# COMMAND ----------

# TRY TO GET IT TO JSON
df = spark.sql("select * from answers") # Define DF
json_string = (df.toJSON().collect())
print(json_string)
spark.conf.set("answers.j",f"{json_string}")
# answer_list = df.collect()
# for row in answer_list: 
#     print(row.id)
#     row_num = row.id
#     solution = row.solution
#     print(row.solution)
#     spark.conf.set(f"answers.{row_num}",solution)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT concat('${answers.j}',"h") 

# COMMAND ----------

spark.conf.set("answers.t","tom")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT ${answers.j} as id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SET

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${answers.1}'== sha2('70',256 ) as id

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION student_test

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION student_test( question_id int, answer STRING)
# MAGIC   
# MAGIC RETURNS BOOLEAN
# MAGIC --RETURNS STRING
# MAGIC RETURN SELECT '${answers.1}'== sha2(answer,256 ) as id
# MAGIC --RETURN SELECT '${concat("answers.",question_id)}'
# MAGIC --RETURN SELECT '${concat("answers.",question_id)}'== sha2(answer,256 ) as id
# MAGIC --RETURN SELECT '${answers.1}'
# MAGIC --RETURN (SELECT sha2(answer, 256)) = (SELECT solution from answers where id=1 limit 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from answers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fun;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table fun as SELECT "{'id':1,'solution':'hey'}" as v

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SET

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SET answers.json = "{'id':1,'solution':'hey'}"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT ${answers.json}:solution where ${answers.json}:id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- DO SOMETHING LIKE THIS
# MAGIC -- ARRAY INDEX is question answer
# MAGIC
# MAGIC SELECT c1:answers[1].value
# MAGIC     FROM VALUES('{ "answers": [ { "value" : "HEY", "id" :1 },{ "value" : "HEY2", "id" :2 } ] }') AS T(c1);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- DO SOMETHING LIKE THIS
# MAGIC -- ARRAY INDEX is question answer
# MAGIC
# MAGIC SELECT c1:answers[0].value
# MAGIC     FROM VALUES('{ "answers": [ { "value" : "HEY", "id" :1 },
# MAGIC                              { "value" : "HEY2", "id" :2 } ] }') AS T(c1);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- DO SOMETHING LIKE THIS
# MAGIC -- ARRAY INDEX is question answer
# MAGIC
# MAGIC SELECT c1:answers.value
# MAGIC     FROM VALUES('{ "answers": [ { "value" : "HEY", "id" :1 },{ "value" : "HEY2", "id" :2 } ] }') AS T(c1);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t:answers[*] from (SELECT '{ "answers": [ { "value" : "HEY", "id" :1 },{ "value" : "HEY2", "id" :2 } ] }' as t) where t:answers[*]:id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c1:item[0].price::double
# MAGIC     FROM VALUES('{ "item": [ { "model" : "basic", "price" : 6.12 },
# MAGIC                              { "model" : "medium", "price" : 9.24 } ] }') AS T(c1);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT v:solution from fun;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT student_test(2, "70")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC myvar = SELECT concat("hello", "world")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION area(x INT, y INT) RETURNS INT
# MAGIC RETURN area.x + y;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT area(2,4)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT student_test(1,"70")

# COMMAND ----------

# print(
#     (spark.sql('select sha2("20", 256)')).collect()[0][0]
#     )

# COMMAND ----------

# Keep THis

def python_student_test(question_id, answer):
    if ((spark.sql(f'select sha2("{answer}", 256)')).collect()[0][0]) == spark.sql(f'SELECT solution from answers where id ="{question_id}"').collect()[0][0]:
        return True
    else:
        return False

# COMMAND ----------

python_student_test(1, "71")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP FUNCTION sql_test

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION sql_test(a INT, b STRING)
# MAGIC   RETURNS BOOLEAN
# MAGIC   LANGUAGE PYTHON
# MAGIC   AS $$
# MAGIC   import spark
# MAGIC     def inner_func(question_id, answer):
# MAGIC       if ((spark.sql(f'select sha2("{answer}", 256)')).collect()[0][0]) == spark.sql(f'SELECT solution from answers where id ="{question_id}"').collect()[0][0]:
# MAGIC         return True
# MAGIC       else:
# MAGIC         return False
# MAGIC     return inner_func(1,"70")
# MAGIC   $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sql_test(1, "70")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION greet(s STRING)
# MAGIC   RETURNS STRING
# MAGIC   LANGUAGE PYTHON
# MAGIC   AS $$
# MAGIC     def greet(name):
# MAGIC       return "Hello " + name + "!"
# MAGIC
# MAGIC     return greet(s) if s else None
# MAGIC   $$

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT solution from answers where id =1;

# COMMAND ----------

python_student_test(1, "70")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT greet("tom")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT student_test1(1, "20")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from answers;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT student_test(2, 70)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE FUNCTION trythis(a int)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC RETURN TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --SET a.myvar = 8;
# MAGIC SELECT ${a.myvar}==8

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # JSON PATH FUN

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE store_data AS SELECT
# MAGIC '{
# MAGIC    "store":{
# MAGIC       "fruit": [
# MAGIC         {"weight":8,"type":"apple"},
# MAGIC         {"weight":9,"type":"pear"}
# MAGIC       ],
# MAGIC       "basket":[
# MAGIC         [1,2,{"b":"y","a":"x"}],
# MAGIC         [3,4],
# MAGIC         [5,6]
# MAGIC       ],
# MAGIC       "book":[
# MAGIC         {
# MAGIC           "author":"Nigel Rees",
# MAGIC           "title":"Sayings of the Century",
# MAGIC           "category":"reference",
# MAGIC           "price":8.95
# MAGIC         },
# MAGIC         {
# MAGIC           "author":"Herman Melville",
# MAGIC           "title":"Moby Dick",
# MAGIC           "category":"fiction",
# MAGIC           "price":8.99,
# MAGIC           "isbn":"0-553-21311-3"
# MAGIC         },
# MAGIC         {
# MAGIC           "author":"J. R. R. Tolkien",
# MAGIC           "title":"The Lord of the Rings",
# MAGIC           "category":"fiction",
# MAGIC           "reader":[
# MAGIC             {"age":25,"name":"bob"},
# MAGIC             {"age":26,"name":"jack"}
# MAGIC           ],
# MAGIC           "price":22.99,
# MAGIC           "isbn":"0-395-19395-8"
# MAGIC         }
# MAGIC       ],
# MAGIC       "bicycle":{
# MAGIC         "price":19.95,
# MAGIC         "color":"red"
# MAGIC       }
# MAGIC     },
# MAGIC     "owner":"amy",
# MAGIC     "zip code":"94025",
# MAGIC     "fb:testid":"1234"
# MAGIC  }' as raw

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from store_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raw:store.fruit[].weight FROM store_data 
# MAGIC --where raw:store.fruit[].type[]="pear";
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raw:store.book[*] from store_data

# COMMAND ----------

#store.book[?(@.price < 10)]

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raw:store.book[*].isbn FROM store_data  ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Index elements
# MAGIC -SELECT raw:store.fruit[0], raw:store.fruit[1] FROM store_data;
# MAGIC --   '{ "weight":8, "type":"apple" }'  '{ "weight":9, "type":"pear" }'
# MAGIC
# MAGIC -- -- Extract subfields from arrays
# MAGIC -- > SELECT raw:store.book[*].isbn FROM store_data;
# MAGIC --   '[ null, "0-553-21311-3", "0-395-19395-8" ]'
# MAGIC
# MAGIC -- -- Access arrays within arrays or structs within arrays
# MAGIC -- > SELECT raw:store.basket[*],
# MAGIC --          raw:store.basket[*][0] first_of_baskets,
# MAGIC --          raw:store.basket[0][*] first_basket,
# MAGIC --          raw:store.basket[*][*] all_elements_flattened,
# MAGIC --          raw:store.basket[0][2].b subfield
# MAGIC --   FROM store_data;
# MAGIC --   basket                       first_of_baskets   first_basket          all_elements_flattened            subfield
# MAGIC --  ---------------------------- ------------------ --------------------- --------------------------------- ----------
# MAGIC --   [                            [                  [                     [1,2,{"b":"y","a":"x"},3,4,5,6]   y
# MAGIC --     [1,2,{"b":"y","a":"x"}],     1,                 1,
# MAGIC --     [3,4],                       3,                 2,
# MAGIC --     [5,6]                        5                  {"b":"y","a":"x"}
# MAGIC --   ]                            ]                  ]
# MAGIC
