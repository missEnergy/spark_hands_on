// Databricks notebook source
// MAGIC %md
// MAGIC ##DataFrame vs Dataset
// MAGIC 
// MAGIC The following example demonstrates how to convert a `DataFrame` to a `Dataset`.
// MAGIC 
// MAGIC ** *Note:* ** *As a reminder, `Datasets` are a Java and Scala concept and brings to those languages the type safety that *<br/>
// MAGIC *is lost with `DataFrame`, or rather, `Dataset[Row]`. Python and R have no such concept because they are loosely typed.*

// COMMAND ----------

// the name and data type of the case class must match the schema they will be converted from.
case class ElectricityRawSchema (electricity:Double, utc_datetime:String, user:String)

// COMMAND ----------

val elecDf = spark.table("electricity_raw")
val elecDs = elecDf.as[ElectricityRawSchema]

// COMMAND ----------

display(elecDf)

// COMMAND ----------

display(elecDs)

// COMMAND ----------

// MAGIC %md
// MAGIC Make note of the data type: **org.apache.spark.sql.Dataset[ElectricityRawSchema]**
// MAGIC 
// MAGIC Compare that to a `DataFrame`: **org.apache.spark.sql.Dataset[Row]**
// MAGIC 
// MAGIC Now when we collect, we won't get an array of `Row` objects but instead an array of `ElectricityRawSchema` objects:

// COMMAND ----------

elecDs.collect()

// COMMAND ----------

//Exercise: convert temperature raw dataframe to a dataset as well

// COMMAND ----------

// MAGIC %md
// MAGIC When we write spark scala code for production purposes, of course we want our code to be well tested!
// MAGIC 
// MAGIC Unit testing allows us to focus on testing small components of functionality with complex dependencies, such as data sources, mocked out

// COMMAND ----------

import org.apache.spark.sql.Dataset

case class ElectricityRawSchema (electricity:Double, utc_datetime:String, user:String)
case class ElectricityProcessedSchema (average_electricity:Integer, date:String, hour:Integer, user:String)

object ElectricityReadAndWriter {
  def read(): Dataset[ElectricityRawSchema] = {
    spark.table("electricity_raw").as[ElectricityRawSchema]
  }
  
  def write(electricityProcessed: Dataset[ElectricityProcessedSchema]): Unit = {
    electricityProcessed
      .write                        
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("electricity_processed")
  }
}

import org.apache.spark.sql.functions._

def transform(electricityRaw: Dataset[ElectricityRawSchema]): Dataset[ElectricityProcessedSchema] = {
  electricityRaw
    .withColumnRenamed("utc_datetime", "datetime")
    .withColumn("date", to_date('datetime).cast("String") as "date")
    .withColumn("hour", hour('datetime) as "hour")
    .groupBy('user, 'date, 'hour)
    .agg(avg("electricity").cast("Integer").as("average_electricity"))
    .as[ElectricityProcessedSchema]
}

def run(): Unit = {
  val ds = ElectricityReadAndWriter.read()
  val dsTransformed = transform(ds)
  ElectricityReadAndWriter.write(dsTransformed)
}

// COMMAND ----------

run()

// COMMAND ----------

// test it transforms with an empty dataset
val emptyDs = spark.emptyDataset[ElectricityRawSchema]

val actual = transform(emptyDs)

//how can we make this cell (=test) fail when the transform doesn't work?

// COMMAND ----------

// Now, we make a test with some dummy data (finish!)

val dummyDs = Seq(
  ElectricityRawSchema(electricity=5.0, utc_datetime="2019-01-01 12:00:00", user="John"),
  ...
)

val expected = val expected = Seq(
  ElectricityProcessedSchema(...),
  ....
)

val actual = transform(dummyDs).collect()

assert(actual === expected)
...

// BTW, Nice package https://github.com/holdenk/spark-testing-base/wiki/DatasetSuiteBase

// COMMAND ----------

// Exercise: can you make a transform method for the join of the hourly temperature and hourly electricty data ? can you test it afterwards ?
