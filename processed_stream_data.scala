// Databricks notebook source
//listing the mounts

display(dbutils.fs.ls("/mnt/mount_databricks_new"))


// COMMAND ----------

//import which used for transformation of the data

import org.apache.spark.sql.functions._


// COMMAND ----------

//reading processed_stream data as a dataframe from s3 bucket

val df = spark.read.format("csv")
          .option("header","true")
        .option("inferschema","true")
        .option("path","dbfs:/mnt/mount_databricks_new/bhavani/processed_data/silver_stream/stream_process.csv")
          .load()

// COMMAND ----------

//display the contents in df
df.show()

// COMMAND ----------

val transform_data = df
        .select(col("dt"),col("device_name"),col("product_type"),
        col("user_id"),col("program_title"), col("country_code"))
        .groupBy(col("dt"),col("device_name"),col("product_type"),col("user_id") as "unique_users",col("program_title"),col("country_code"))
        .agg(count(col("program_title")) as "content_count")
        .withColumn("load_date", current_date())
        .withColumn("year", year(col("load_date")))
        .withColumn("month", month(col("load_date")))
        .withColumn("day", dayofmonth(col("load_date")))
        .sort(col("content_count").desc) 

// COMMAND ----------

transform_data.show()

// COMMAND ----------

//writing the data into the structured layer of s3 with partitions of year,month,date


transform_data.repartition(1).write.option("header","true").partitionBy("year","month","day")
.csv("dbfs:/mnt/mount_databricks_new/bhavani/structured_data/gold_stream_data")
