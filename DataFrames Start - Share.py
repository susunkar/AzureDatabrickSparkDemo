# Databricks notebook source
# Create multi column DataFrame from RDD
employees = sc.parallelize(
                            [
                                (1, "John", 10000),
                                (2, "Fred", 20000),
                                (3, "Anna", 30000),
                                (4, "James", 40000),
                                (5, "Mohit", 50000)
                            ]
                          )

employeesDF = employees.toDF()

employeesDF.show()

# COMMAND ----------

employeesDF = employeesDF.toDF("id", "name", "salary")

employeesDF.show()

# COMMAND ----------

# Filter data

newdf = (
            employeesDF
                .where("salary > 20000")
        )

display(
  newdf
)

# COMMAND ----------

# MAGIC %md ###Directly Accessing Data Lake with Access Key

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.<data lake name>.dfs.core.windows.net",
  
    "<access key>")

# COMMAND ----------

dbutils.fs.ls("abfss://<container name>@<data lake name>.dfs.core.windows.net/<path>")

# COMMAND ----------

# MAGIC %md ###Mount Data Lake

# COMMAND ----------

#Unmount if already mounted
dbutils.fs.unmount("/mnt/mstrainingdatalake")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<client id>",
           "fs.azure.account.oauth2.client.secret": "<client secret>",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container name>@<data lake name>.dfs.core.windows.net/",
  mount_point = "/mnt/mstrainingdatalake",
  extra_configs = configs)

# COMMAND ----------

# Run this, and next command if mounting gives an error
spark.conf.set(
  "fs.azure.account.key.<data lake name>.dfs.core.windows.net",
  "<access key>")

# COMMAND ----------

spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://<container name>@<data lake name>.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %fs ls /mnt/mstrainingdatalake

# COMMAND ----------

display(
  dbutils.fs.ls("/mnt/mstrainingdatalake")
)

# COMMAND ----------

# MAGIC %md ###Magic Commands
# MAGIC 
# MAGIC Following is a magic command

# COMMAND ----------

# MAGIC %md ##Working with DBUtils

# COMMAND ----------

dbutils.fs.head("/mnt/mstrainingdatalake/Raw/GreenTaxis_201911.csv")

# COMMAND ----------

# MAGIC %fs head /mnt/mstrainingdatalake/Raw/GreenTaxis_201911.csv

# COMMAND ----------

# MAGIC %md ##Working with DataFrames

# COMMAND ----------

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .csv("/mnt/mstrainingdatalake/Raw/GreenTaxis_201911.csv")
              )

display(greenTaxiDF)

# COMMAND ----------

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .csv("/mnt/mstrainingdatalake/Raw/GreenTaxis_201911.csv")
              )

display(greenTaxiDF)

# COMMAND ----------

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv("/mnt/mstrainingdatalake/Raw/GreenTaxis_201911.csv")
              )

display(greenTaxiDF)
