# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ecommerce.bronze;
# MAGIC create schema if not exists ecommerce.silver;
# MAGIC create schema if not exists ecommerce.gold;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC show databases from ecommerce;

# COMMAND ----------

