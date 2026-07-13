# Databricks notebook source
LEASE_DATA_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA.parquet"
LEASE_DATA_df = spark.read.parquet(LEASE_DATA_mdh)
