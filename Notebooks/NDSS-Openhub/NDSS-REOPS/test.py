# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("HelloApp").getOrCreate()

# Print "hello"
print("hello")