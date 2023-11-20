# Databricks notebook source
# Keep THis

def python_student_test(question_id, answer):
    if ((spark.sql(f'select sha2("{answer}", 256)')).collect()[0][0]) == spark.sql(f'SELECT solution from answers where id ="{question_id}"').collect()[0][0]:
        return True
    else:
        return False
