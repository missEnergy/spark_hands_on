// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ### The Data Sources
// MAGIC * Now we are going to load the csv files we just uploaded
// MAGIC * We can use **&percnt;fs ls ...** to view the files on the DBFS.

// COMMAND ----------

// MAGIC %fs ls FileStore/tables

// COMMAND ----------

// MAGIC %md
// MAGIC ### And check out what is in the file...

// COMMAND ----------

// MAGIC %fs head FileStore/tables/electricity_flow.csv

// COMMAND ----------

// MAGIC %md
// MAGIC What can you infer about the header, separation character & data types ?
// MAGIC 
// MAGIC Knowing those details, we can read in the "CSV" file.

// COMMAND ----------

val csvFile = "/FileStore/tables/electricity_flow.csv"

val df = spark.read       // The DataFrameReader
  .option("sep", ",")         // Comma-separator (=default)
  .csv(csvFile)               // Creates a DataFrame from CSV after reading in the file

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC We can see from the schema that...
// MAGIC * there are three columns
// MAGIC * the column names **_c0**, **_c1**, and **_c2** (automatically generated names)
// MAGIC * all three columns are **strings**
// MAGIC 
// MAGIC And if we take a quick peek at the data, we can see that line #1 contains the headers and not data:

// COMMAND ----------

spark.read                    // The DataFrameReader
  .option("header", "true")   // Use first line of all files as header
  .csv(csvFile)               // Creates a DataFrame from CSV after reading in the file
  .printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC A couple of notes about this iteration:
// MAGIC * again, only one job
// MAGIC * there are three columns
// MAGIC * all three columns are **strings**
// MAGIC * the column names are specified
// MAGIC 
// MAGIC A "peek" at the first line of the file is all that the reader needs to determine the number of columns and the name of each column.
// MAGIC 
// MAGIC Before going on, make a note of the duration of the previous call

// COMMAND ----------

spark.read                        // The DataFrameReader
  .option("header", "true")       // Use first line of all files as header
  .option("inferSchema", "true")  // Automatically infer data types
  .csv(csvFile)                   // Creates a DataFrame from CSV after reading in the file
  .printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Review: Reading CSV w/InferSchema
// MAGIC * we still have three columns
// MAGIC * all three columns have their proper names
// MAGIC * two jobs were executed (not one as in the previous example)
// MAGIC * our three columns now have distinct data types
// MAGIC 
// MAGIC **Question:** Why were there two jobs?
// MAGIC 
// MAGIC **Question:** How long did the last job take?
// MAGIC 
// MAGIC **Question:** Why did it take so much longer?

// COMMAND ----------

// MAGIC %md
// MAGIC ### To avoid this, let's define the schema before we read

// COMMAND ----------

// Required for StructField, StringType, IntegerType, etc.
import org.apache.spark.sql.types._

val csvSchema = StructType(
  List(
    StructField("electricity", DoubleType),
    StructField("utc_datetime", StringType),
    StructField("user", StringType)
  )
)

// COMMAND ----------

spark.read                    // The DataFrameReader
  .option("header", "true")   // Ignore line #1 - it's a header
  .schema(csvSchema)          // Use the specified schema
  .csv(csvFile)               // Creates a DataFrame from CSV after reading in the file
  .printSchema()
        

// COMMAND ----------

// MAGIC %md
// MAGIC ### Review: Reading CSV w/ User-Defined Schema
// MAGIC * We still have three columns
// MAGIC * All three columns have their proper names
// MAGIC * Zero jobs were executed
// MAGIC * Our three columns now have distinct data types
// MAGIC 
// MAGIC **Question:** Why were there no jobs?
// MAGIC 
// MAGIC **Question:** What is different about the data types of these columns compared to the previous exercise & why?
// MAGIC 
// MAGIC **Question:** Do I need to indicate that the file has a header?
// MAGIC 
// MAGIC **Question:** Do the declared column names need to match the columns in the header of the csv file?

// COMMAND ----------

val csvDF = spark.read
  .option("header", "true")
  .schema(csvSchema)
  .csv(csvFile)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have a DataFrame, we can write it back out as Parquet files or other various formats.

// COMMAND ----------

val fileName = "/FileStore/tables/electricity"

csvDF.write                        // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(fileName)               // Write DataFrame to Parquet files

// COMMAND ----------

// MAGIC %fs ls FileStore/tables/electricity

// COMMAND ----------

// Create a view or table

val temp_table_name = "electricity_csv"

csvDF.createOrReplaceTempView(temp_table_name)

//https://stackoverflow.com/questions/44011846/how-does-createorreplacetempview-work-in-spark

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC /* Query the created temp table in a SQL cell */
// MAGIC 
// MAGIC select * from `electricity_csv`

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # or python
// MAGIC 
// MAGIC display(spark.read.table("electricity_csv"))

// COMMAND ----------

// or save to the Hive metastore as a table available for access later
csvDF
  .write                        
  .format("parquet")
  .saveAsTable("electricity_raw")

// COMMAND ----------

// MAGIC %md
// MAGIC   ##Reading CSV
// MAGIC - `spark.read.csv(..)`
// MAGIC - There are a large number of options when reading CSV files including headers, column separator, escaping, etc.
// MAGIC   - We can allow Spark to infer the schema at the cost of first reading in the entire file.
// MAGIC   - Large CSV files should always have a schema pre-defined.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading Parquet
// MAGIC - `spark.read.parquet(..)`
// MAGIC - Parquet files are the preferred file format for big-data.
// MAGIC - It is a columnar file format.
// MAGIC - It is a splittable file format.
// MAGIC - It offers a lot of performance benefits over other formats including predicate pushdown.
// MAGIC - Unlike CSV, the schema is read in, not inferred.
// MAGIC - Reading the schema from Parquet's metadata can be extremely efficient.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading Tables
// MAGIC - `spark.read.table(..)`
// MAGIC - The Databricks platform allows us to register a huge variety of data sources as tables via the Databricks UI.
// MAGIC - Any `DataFrame` (from CSV, Parquet, whatever) can be registered as a temporary view.
// MAGIC - Tables/Views can be loaded via the `DataFrameReader` to produce a `DataFrame`
// MAGIC - Tables/Views can be used directly in SQL statements.

// COMMAND ----------

// MAGIC %md 
// MAGIC # Exercise
// MAGIC Also create a `temperature_raw` table!

// COMMAND ----------


