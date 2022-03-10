// Databricks notebook source
//mounting the data to connect with s3 bucket using access and secret keys

val access_key = "AKIA22UAFDPYYDZBIXBL"
val secret_key = "hklNXmXCRwyqgN/5FjBvxYUzchUrZ3egN39VT4Zs"
val encoded_secret_key =  secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "mount_databricks_new"
dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")



// COMMAND ----------

//listing the mounts

display(dbutils.fs.ls("/mnt/mount_databricks_new"))

// COMMAND ----------

//import which used for transformation of the data

import org.apache.spark.sql.functions._

// COMMAND ----------

//reading boardcast data as a dataframe from s3 bucket

val df = spark.read.format("csv")
          .option("header","true")
        .option("inferschema","true")
        .option("path","dbfs:/mnt/mount_databricks_new/bhavani/raw/broadcast_right.csv")
          .load()

// COMMAND ----------

//displaying the contents of the dataframe

df.show()

// COMMAND ----------

// transformation on particular columns and add a new column in the boardcast dataframe.


val trans_data =  df.withColumn("broadcast_right_vod_type",lower(col("broadcast_right_vod_type")))
    .withColumn("country_code",
        when(col("broadcast_right_region") === "Denmark", "dk")
        .when(col("broadcast_right_region") === "Baltics", "ba")
        .when(col("broadcast_right_region") === "Bulgaria", "bg")
        .when(col("broadcast_right_region") === "Estonia", "ee")
        .when(col("broadcast_right_region") === "Finland", "fl")
        .when(col("broadcast_right_region") === "Latvia", "lv")
        .when(col("broadcast_right_region") === "Lithuania", "lt")
        .when(col("broadcast_right_region") === "Nordic", "nd")
        .when(col("broadcast_right_region") === "Norway", "no")
        .when(col("broadcast_right_region") === "Russia", "ru")
        .when(col("broadcast_right_region") === "Serbia", "rs")
        .when(col("broadcast_right_region") === "Slovenia", "si")
        .when(col("broadcast_right_region") === "Sweden", "se")
        .when(col("broadcast_right_region") === "VNH Region Group","vnh")
        .when(col("broadcast_right_region") === "Viasat History Region Group","vh")
        .otherwise("nocode"))

// COMMAND ----------

trans_data.show()

// COMMAND ----------

//writing the data into s3 bucket and repartition the data as 1 file in the bucket

trans_data.repartition(1).write.option("header","true").csv("dbfs:/mnt/mount_databricks_new/bhavani/processed_data/silver_broadcast")
