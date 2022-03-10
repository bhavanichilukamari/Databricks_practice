// Databricks notebook source
//listing the mounts

display(dbutils.fs.ls("/mnt/mount_databricks_new"))

// COMMAND ----------

//reading streamed data from s3 bucket


val df1 = spark.read.format("csv")
          .option("delimiter",";")
          .option("header","true")
        .option("inferschema","true")
        .option("path","dbfs:/mnt/mount_databricks_new/bhavani/raw/started_streams.csv")
          .load()

// COMMAND ----------

df1.show()

// COMMAND ----------

//writing the data into s3 bucket and repartition the data as 1 file in the bucket

df1.repartition(1).write.option("header","true").csv("dbfs:/mnt/mount_databricks_new/bhavani/processed_data/silver_stream")
