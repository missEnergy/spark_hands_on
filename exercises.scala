// Databricks notebook source
// MAGIC %md
// MAGIC Dit moeten de trainees dan zelf doen --> temperature

// COMMAND ----------

val csvFile2 = "/FileStore/tables/inside_temperature.csv"

import org.apache.spark.sql.types._

val csvSchema2 = StructType(
  List(
    StructField("inside", DoubleType),
    StructField("utc_datetime", StringType),
    StructField("user", StringType)
  )
)

val csvDF2 = spark.read
  .option("header", "true")
  .schema(csvSchema2)
  .csv(csvFile2)

// COMMAND ----------

csvDF2
  .write
  .format("parquet")
  .saveAsTable("temperature_raw")

// COMMAND ----------

//Solution
import org.apache.spark.sql.functions._

val elecDf = spark.table("electricity_raw")
  .withColumnRenamed("utc_datetime", "datetime")
  .withColumn("date", to_date('datetime).cast("String") as "date")
  .withColumn("hour", hour('datetime) as "hour")
  .groupBy('user, 'date, 'hour)
  .agg(avg("electricity").as("average_electricity"))

val tempDf = spark.table("temperature_raw")
  .withColumnRenamed("utc_datetime", "datetime")
  .withColumn("date", to_date('datetime).cast("String") as "date")
  .withColumn("hour", hour('datetime) as "hour")
  .groupBy('user, 'date, 'hour)
  .agg(avg("inside").as("average_temperature"))

elecDf
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .saveAsTable("electricity_processed")

tempDf
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .saveAsTable("temperature_processed")

//COMMAND ----------

// MAGIC What are good reasons to cache ?
// MAGIC RDD re-use in iterative machine learning applications
// MAGIC RDD re-use in standalone Spark applications
// MAGIC When RDD computation is expensive, caching can help in reducing the cost of recovery in the case one executor fails


// MAGIC What are disadvantages to cache ?
// MAGIC take space from other computations
// MAGIC increase risk of memory failure
// MAGIC expensive operation
// preventing narrow dependencies to be computed in 1 task

//checkpointing !
//  What is the difference ?
// external storage instead of memory/disk, breaking lineage, survives beyond duration of a spark application
//
//Why would you use it instead of caching ?
// not using Spark memory, no recomputation when worker node fails
// when the cost of failure and recomputation is of more concern than additional space in external storage, solve out of memory issues
// can be expensive write!

//SOLUTION
def mapDayOfWeek = udf { day:String =>
  day match {
    case "Mon" => "1-Mon"
    case "Tue" => "2-Tue"
    case "Wed" => "3-Wed"
    case "Thu" => "4-Thu"
    case "Fri" => "5-Fri"
    case "Sat" => "6-Sat"
    case "Sun" => "7-Sun"
    case _ => "UNKNOWN"
  }
}

// SOLUTION
display(newDf
  .withColumn("weekday", date_format('date, "u-E"))
  .groupBy('weekday)
  .agg(avg('electricity).as("average_electricity"))
  .orderBy('weekday)
)

// SOLUTION
def mapTempMode = udf { temp:Double =>
  if (temp < 19.0) "COLD" else if (temp < 21.0) "COMFORTABLE" else "HOT"
}


// SOLUTION
val emptyDs = spark.emptyDataset[ElectricityRawSchema]

val actual = transform(emptyDs).collect()

assert(actual.length == 0)


// SOLUTION

import eu.toon.storage._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.ColumnName
val df = spark.table("electricity_processed")
df.filter('user === "6c62cd5594")

val df_filtered = df
  .filter('utc_date === "2019-01-01") // syntax sugar for the option below
  .filter(Symbol("utc_date") === "2019-01-01") // implicitly converted to Column type
  .filter($"utc_date" === "2019-01-01") // implicitly converted to Column type
  .filter(new ColumnName("utc_date") === "2019-01-01") // ColumnName extends Column type
  .filter("utc_date == '2019-01-01'")
  .filter(col("utc_date") === "2019-01-01") // requires import of org.apache.spark.sql.functions.col
  .where(df("utc_date") === "2019-01-01") // not possible to chain directly to spark.table because of recursion
  .filter(df.col("utc_date") === "2019-01-01")
  .as[ElectricityMeteringSchema]
  .filter(d => d.utc_date == "2019-01-01") // only works on DS, not on DF

display(df_filtered)

