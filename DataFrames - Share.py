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

# COMMAND ----------

# MAGIC %md ##Applying Schemas

# COMMAND ----------

# Create schema for Green Taxi Data

from pyspark.sql.functions import *
from pyspark.sql.types import *
  
greenTaxiSchema = (
            StructType()               
               .add("VendorId", "integer")
               .add("lpep_pickup_datetime", "timestamp")
               .add("lpep_dropoff_datetime", "timestamp")
               .add("store_and_fwd_flag", "string")
               .add("RatecodeID", "integer")
               .add("PULocationID", "integer")
               .add("DOLocationID", "integer")
  
              .add("passenger_count", "integer")
              .add("trip_distance", "double")
              .add("fare_amount", "double")
              .add("extra", "double")
              .add("mta_tax", "double")
              .add("tip_amount", "double")
  
              .add("tolls_amount", "double")
              .add("ehail_fee", "double")
              .add("improvement_surcharge", "double")
              .add("total_amount", "double")
              .add("payment_type", "integer")
              .add("trip_type", "integer")
         )

# COMMAND ----------

# Read csv file
greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .schema(greenTaxiSchema)
                    .csv("/mnt/mstrainingdatalake/Raw/GreenTaxis_201911.csv")
              )

display(greenTaxiDF)

# COMMAND ----------

# MAGIC %md ##Analyzing Data

# COMMAND ----------

display(
    greenTaxiDF.describe(
                             "passenger_count",                                     
                             "trip_distance"                                     
                        )
)

# COMMAND ----------

# MAGIC %md ##Cleaning Data

# COMMAND ----------

# Display the count before filtering
print("Before filter = " + str(greenTaxiDF.count()))

# Filter inaccurate data - ACCURACY CHECK
greenTaxiDF = (
                  greenTaxiDF
                          .where("passenger_count > 0")
                          .filter(col("trip_distance") > 0.0)
)

# Display the count after filtering
print("After filter = " + str(greenTaxiDF.count()))

# COMMAND ----------

# Display the count before filtering
print("Before filter = " + str(greenTaxiDF.count()))

# Drop rows with nulls - COMPLETENESS CHECK
greenTaxiDF = (
                  greenTaxiDF
                          .na.drop('all')
              )

# Display the count after filtering
print("After filter = " + str(greenTaxiDF.count()))

# COMMAND ----------

# Map of default values
defaultValueMap = {'payment_type': 5, 'RateCodeID': 1}

# Replace nulls with default values - COMPLETENESS CHECK
greenTaxiDF = (
                  greenTaxiDF
                      .na.fill(defaultValueMap)
              )

# COMMAND ----------

# Display the count before filtering
print("Before filter = " + str(greenTaxiDF.count()))

# Drop duplicate rows - UNIQUENESS CHECK
greenTaxiDF = (
                  greenTaxiDF
                          .dropDuplicates()
              )

# Display the count after filtering
print("After filter = " + str(greenTaxiDF.count()))

# COMMAND ----------

# Display the count before filtering
print("Before filter = " + str(greenTaxiDF.count()))

# Drop duplicate rows - TIMELINESS CHECK
greenTaxiDF = (
                  greenTaxiDF
                          .where("lpep_pickup_datetime >= '2019-11-01' AND lpep_dropoff_datetime < '2019-12-01'")
              )

# Display the count after filtering
print("After filter = " + str(greenTaxiDF.count()))

# COMMAND ----------

# Chain all operations together

greenTaxiDF = (
                  spark
                    .read                     
                    .option("header", "true")
                    .schema(greenTaxiSchema)
                    .csv("/mnt/mstrainingdatalake/Raw/GreenTaxis_201911.csv")
              )

greenTaxiDF = (
                  greenTaxiDF
                          .where("passenger_count > 0")
                          .filter(col("trip_distance") > 0.0)
  
                          .na.drop(how='all')
  
                          .na.fill(defaultValueMap)
  
                          .dropDuplicates()
  
                          .where("lpep_pickup_datetime >= '2019-11-01' AND lpep_dropoff_datetime < '2019-12-01'")
              )

# COMMAND ----------

# MAGIC %md ##Applying Transformations

# COMMAND ----------

greenTaxiDF = (
                  greenTaxiDF

                        # Select only limited columns
                        .select(
                                  col("VendorID"),
                                  col("passenger_count").alias("PassengerCount"),
                                  col("trip_distance").alias("TripDistance"),
                                  col("lpep_pickup_datetime").alias("PickupTime"),                          
                                  col("lpep_dropoff_datetime").alias("DropTime"), 
                                  col("PUlocationID").alias("PickupLocationId"), 
                                  col("DOlocationID").alias("DropLocationId"), 
                                  col("RatecodeID"), 
                                  col("total_amount").alias("TotalAmount"),
                                  col("payment_type").alias("PaymentType")
                               )
              )

greenTaxiDF.printSchema

# COMMAND ----------

# Create a derived column - Trip time in minutes
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime"))) 
                                                    / 60
                                             )
                                   )
              )

greenTaxiDF.printSchema

# COMMAND ----------

# Create a derived column - Trip type, and drop SR_Flag column
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripType", 
                                        when(
                                                col("RatecodeID") == 6,
                                                  "SharedTrip"
                                            )
                                        .otherwise("SoloTrip")
                                   )
                        .drop("RatecodeID")
              )

greenTaxiDF.printSchema

# COMMAND ----------

# Create derived columns for year, month and day
greenTaxiDF = (
                  greenTaxiDF
                        .withColumn("TripYear", year(col("PickupTime")))
                        .withColumn("TripMonth", month(col("PickupTime")))
                        .withColumn("TripDay", dayofmonth(col("PickupTime")))
              )

greenTaxiDF.printSchema

# COMMAND ----------

display(greenTaxiDF)

# COMMAND ----------

greenTaxiGroupedDF = (
                          greenTaxiDF
                            .groupBy("TripDay")
                            .agg(sum("TotalAmount").alias("total"))
                            .orderBy(col("TripDay").desc())
                     )
    
display(greenTaxiGroupedDF)

# COMMAND ----------

# MAGIC %fs head /mnt/mstrainingdatalake/Raw/TaxiZones.csv

# COMMAND ----------

# Read payment types json file
taxiZonesDF = (
                  spark
                      .read
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .csv("/mnt/mstrainingdatalake/Raw/TaxiZones.csv")
              )

display(taxiZonesDF)

# COMMAND ----------

# Create a dataframe joining FHV trip data with bases

greenTaxiWithZonesDF = (
                          greenTaxiDF.alias("g")
                                     .join(taxiZonesDF.alias("t"),                                               
                                               col("t.LocationId") == col("g.PickupLocationId"),
                                              "inner"
                                          )
                       )

display(greenTaxiWithZonesDF)

# COMMAND ----------

# MAGIC %md ##Exercise

# COMMAND ----------

# EXERCISE - JOIN greenTaxiWithZonesDF with TaxiZones on DropLocationId. And group by PickupZone and DropZone, and provide average of TotalAmount.

# COMMAND ----------

# MAGIC %md ##Working with Spark SQL

# COMMAND ----------

# Create a local temp view
greenTaxiDF.createOrReplaceTempView("GreenTaxiTripData")

# COMMAND ----------

display(
  spark.sql("SELECT PassengerCount, PickupTime FROM GreenTaxiTripData WHERE PickupLocationID = 1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT PassengerCount, PickupTime 
# MAGIC FROM GreenTaxiTripData 
# MAGIC WHERE PickupLocationID = 1

# COMMAND ----------

# MAGIC %md ##Writing Output to Data Lake

# COMMAND ----------

# Load the dataframe as CSV to storage
(
    greenTaxiDF  
        .write
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
        .mode("overwrite")
        .csv("/mnt/mstrainingdatalake/Facts/GreenTaxiFact.csv")
)

# COMMAND ----------

# Load the dataframe as parquet to storage
(
    greenTaxiDF  
      .write
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
      .mode("overwrite")
      .parquet("/mnt/mylantraining/GreenTaxiFact.parquet")
)

# COMMAND ----------

# MAGIC %md ###Reading JSON

# COMMAND ----------

paymentTypes = (
                    spark
                        .read
                        .json("/mnt/mstrainingdatalake/Raw/PaymentTypes.json")
)

display(paymentTypes)

# COMMAND ----------

# MAGIC %md ###Working with Spark SQL and Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS TaxiServiceWarehouse

# COMMAND ----------

(
  greenTaxiDF
    .write
    .mode("overwrite")
    .saveAsTable("TaxiServiceWarehouse.FactGreenTaxiTripDataManaged")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM TaxiServiceWarehouse.FactGreenTaxiTripDataManaged
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED TaxiServiceWarehouse.FactGreenTaxiTripDataManaged

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE TaxiServiceWarehouse.FactGreenTaxiTripDataManaged

# COMMAND ----------

# Store the DataFrame as an Unmanaged Table
(greenTaxiDF
    .write
    .mode("overwrite")
    .option("path", "/mnt/mstrainingdatalake/DimensionalModel/Facts/GreenTaxiFact.parquet")
    #.format("csv")   /* Default format is parquet */
    .saveAsTable("TaxiServiceWarehouse.FactGreenTaxiTripData") 
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED TaxiServiceWarehouse.FactGreenTaxiTripData

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE TaxiServiceWarehouse.FactGreenTaxiTripData

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaxiServiceWarehouse.FactGreenTaxiTripData
# MAGIC     USING parquet
# MAGIC     OPTIONS 
# MAGIC     (
# MAGIC         path "/mnt/mstrainingdatalake/DimensionalModel/Facts/GreenTaxiFact.parquet"
# MAGIC     )

# COMMAND ----------


