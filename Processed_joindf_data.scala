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

display(df)

// COMMAND ----------

//reading boardcast data as a dataframe from s3 bucket

val df1 = spark.read.format("csv")
          .option("header","true")
        .option("inferschema","true")
        .option("path","dbfs:/mnt/mount_databricks_new/bhavani/processed_data/silver_broadcast/broadcast_process.csv")
          .load()



// COMMAND ----------

display(df1)

// COMMAND ----------

// actual transformation to get the  users with product type of tvod and est

 val joined_df = df.join(df1,df("house_number") === df1("house_number") && 
        df("country_code") === df1("country_code"),"inner")
 .drop(df.col("dt"))
 .drop(df.col("house_number"))
.drop(df.col("country_code"))
        .select("dt","time",
"device_name",
"house_number",
"user_id",
"country_code",
"program_title",
"season",
"season_episode",
"genre",
"product_type",
"broadcast_right_start_date",
"broadcast_right_end_date")
.where(df("product_type") === "tvod" || df("product_type") === "est")
        .withColumn("load_dates",current_date())
        .withColumn("year",year(col("load_dates")))
        .withColumn("month",month(col("load_dates")))
        .withColumn("day",dayofmonth(col("load_dates")))

// COMMAND ----------

display(joined_df)

//replacing null values in season and season episode with 0

val null_df = joined_df.withColumn("season",expr("coalesce(season,0)"))
              .withColumn("season_episode",expr("coalesce(season_episode,0)"))

//display the df after replacing

display(null_df)

// COMMAND ----------

//writing the data into the structured layer of s3 with partitions of year,month,date


joined_df.repartition(1).write.option("header","true").partitionBy("year","month","day")
.csv("dbfs:/mnt/mount_databricks_new/bhavani/structured_data/gold_joined_data")
