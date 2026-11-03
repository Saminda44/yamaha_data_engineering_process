# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG `yamaha-catalog`; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS `yamaha-catalog`.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS `yamaha-catalog`.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS `yamaha-catalog`.gold;