// Databricks notebook source
// MAGIC %md
// MAGIC ## A Fresh Start
// MAGIC For this section, we need to clear the existing cache.
// MAGIC 
// MAGIC There are several ways to accomplish this:
// MAGIC   * Remove each cache one-by-one, fairly problematic (`unpersist()`, no `uncache`)
// MAGIC   * Restart the cluster - takes a fair while to come back online
// MAGIC   * Just blow the entire cache away - this will affect every user on the cluster!!

// COMMAND ----------

spark.catalog.clearCache()

// COMMAND ----------

// MAGIC %sql
// MAGIC CLEAR CACHE

// COMMAND ----------

// MAGIC %md
// MAGIC This will ensure that any caches produced by other labs/notebooks will be removed.
// MAGIC 
// MAGIC Next, open the **Spark UI** and go to the **Storage** tab - it should be empty.
// MAGIC 
// MAGIC Now, we are going to work with the temperature data again, from scratch.

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/inside_temperature.csv

// COMMAND ----------

val csvFile = "/FileStore/tables/inside_temperature.csv"
import org.apache.spark.sql.types._

val csvSchema = StructType(
  List(
    StructField("inside", DoubleType),
    StructField("utc_datetime", StringType),
    StructField("user", StringType)
  )
)

val df = spark.read
  .option("header", "true")
  .schema(csvSchema)
  .csv(csvFile)

// COMMAND ----------

df.count

// COMMAND ----------

// MAGIC %md
// MAGIC The file is currently in our object store, which means each time you scan through it, your Spark cluster has to read the data remotely over the network.

// COMMAND ----------

// MAGIC %md
// MAGIC The DataFrame contains ~ 2.5 million rows.
// MAGIC 
// MAGIC Make a note of how long the previous operation takes.
// MAGIC 
// MAGIC Re-run it several times trying to establish an average.
// MAGIC 
// MAGIC Let's try a slightly more complicated operation, such as sorting, which induces an "expensive" shuffle.

// COMMAND ----------

df
  .orderBy('inside)
  .count()

// COMMAND ----------

// MAGIC %md
// MAGIC Again, make note of how long the operation takes.
// MAGIC 
// MAGIC Rerun it several times to get an average.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Every time we re-run these operations, it goes all the way back to the original data store.
// MAGIC 
// MAGIC This requires pulling all the data across the network for every execution.
// MAGIC 
// MAGIC In many/most cases, this network IO is the most expensive part of a job.

// COMMAND ----------

// MAGIC %md
// MAGIC ## cache()
// MAGIC 
// MAGIC We can avoid all of this overhead by caching the data on the executors.
// MAGIC 
// MAGIC Go ahead and run the following command.
// MAGIC 
// MAGIC Make note of how long it takes to execute.

// COMMAND ----------


df.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC The `cache(..)` operation doesn't do anything other than mark a `DataFrame` as cacheable.
// MAGIC 
// MAGIC And while it does return an instance of `DataFrame` it is not technically a transformation or action
// MAGIC 
// MAGIC In order to actually cache the data, Spark has to process over every single record.
// MAGIC 
// MAGIC As Spark processes every record, the cache will be materialized.
// MAGIC 
// MAGIC A very common method for materializing the cache is to execute a `count()`.
// MAGIC 
// MAGIC **BUT BEFORE YOU DO** Check the **Spark UI** to make sure it's still empty even after calling `cache()`.

// COMMAND ----------


df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC The last `count()` will take a little longer than normal.
// MAGIC 
// MAGIC It has to perform the cache and do the work of materializing the cache.
// MAGIC 
// MAGIC Now the dataframe is cached **AND** the cache has been materialized.
// MAGIC 
// MAGIC Before we rerun our queries, check the **Spark UI** and the **Storage** tab.
// MAGIC 
// MAGIC Now, run the two queries and compare their execution time to the ones above.

// COMMAND ----------


df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Faster, right?
// MAGIC 
// MAGIC All of our data is being stored in RAM on the executors.
// MAGIC 
// MAGIC We are no longer making network calls.

// COMMAND ----------

// MAGIC %md
// MAGIC There is no universal answer when choosing what should be cached. Caching an intermediate result can dramatically improve performance and it’s tempting to cache a lot of things.
// MAGIC 
// MAGIC However, due to Spark’s caching strategy (in-memory then swap to disk) the cache can end up in a slightly slower storage. Also, using that storage space for caching purposes means that it’s not available for processing.
// MAGIC 
// MAGIC In the end, caching might cost more than simply reading the DataFrame.

// COMMAND ----------

// MAGIC %md
// MAGIC Now that the `DataFrame` is cached in memory let's go review the **Spark UI** in more detail.
// MAGIC 
// MAGIC In the **RDDs** table, you should see only one record - multiple if you reran the `cache()` operation.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's review the **Spark UI**'s **Storage** details
// MAGIC * RDD Name
// MAGIC * Storage Level
// MAGIC * Cached Partitions
// MAGIC * Fraction Cached
// MAGIC * Size in Memory
// MAGIC * Size on Disk

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's dig deeper into the storage details...
// MAGIC 
// MAGIC Click on the link in the **RDD Name** column to open the **RDD Storage Info**.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's review the **RDD Storage Info**
// MAGIC * Size in Memory
// MAGIC * Size on Disk
// MAGIC * Executors

// MAGIC If you recall...
// MAGIC * We should have 8 partitions.
// MAGIC * With 74MB of data divided into 8 partitions...
// MAGIC * The first seven partitions should be 9.6MB each.
// MAGIC * The last partition will be significantly smaller than the others.

// MAGIC **Question:** What is the difference between **Size in Memory** and **Size on Disk**?

// COMMAND ----------

// MAGIC %md
// MAGIC ##persist()
// MAGIC 
// MAGIC `cache()` is just an alias for `persist()`
// MAGIC 
// MAGIC Let's take a look at the API docs for `persist`
// MAGIC 
// MAGIC `persist()` allows one to specify an additional parameter (storage level) indicating how the data is cached:
// MAGIC * DISK_ONLY
// MAGIC * DISK_ONLY_2
// MAGIC * MEMORY_AND_DISK
// MAGIC * MEMORY_AND_DISK_2
// MAGIC * MEMORY_AND_DISK_SER
// MAGIC * MEMORY_AND_DISK_SER_2
// MAGIC * MEMORY_ONLY
// MAGIC * MEMORY_ONLY_2
// MAGIC * MEMORY_ONLY_SER
// MAGIC * MEMORY_ONLY_SER_2
// MAGIC * OFF_HEAP
// MAGIC 
// MAGIC ** *Note:* ** *The default storage level for...*
// MAGIC * *RDDs are **MEMORY_ONLY**.*
// MAGIC * *DataFrames are **MEMORY_AND_DISK**.*
// MAGIC * *Streaming is **MEMORY_AND_DISK_2**.*

// COMMAND ----------

// MAGIC %md
// MAGIC Before we can use the various storage levels, it's necessary to import the enumerations...

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) RDD Name
// MAGIC 
// MAGIC If you haven't noticed yet, the **RDD Name** on the **Storage** tab in the **Spark UI** is a big ugly name.
// MAGIC 
// MAGIC It's a bit hacky, but there is a workaround for assigning a name.
// MAGIC 0. Create your `DataFrame`.
// MAGIC 0. From that `DataFrame`, create a temporary view with your desired name.
// MAGIC 0. Specifically, cache the table via the `SparkSession` and its `Catalog`.
// MAGIC 0. Materialize the cache.

// COMMAND ----------

df.unpersist() // get rid of the long unreadable name

df.createOrReplaceTempView("temp_tmp")
spark.catalog.cacheTable("temp_tmp")

df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC And now to clean up after ourselves...

// COMMAND ----------

df.unpersist()

// COMMAND ----------

// MAGIC %sql 
// MAGIC CLEAR CACHE

// COMMAND ----------

// MAGIC %md
// MAGIC What are good reasons to cache ?
// MAGIC 
// MAGIC What are disadvantages of cache ?

// COMMAND ----------

// MAGIC %md
// MAGIC Next to caching, there is also 
// MAGIC 
// MAGIC ##checkpointing !
// MAGIC 
// MAGIC What is the difference ?
// MAGIC 
// MAGIC Why would you use it instead of caching ?

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Broadcast Join
// MAGIC 
// MAGIC We saw in the `Joins` notebook that joins can cause a shuffle of partitions. To partly avoid this, you can use a `broadcast` join. In broadcast join, the smaller table will be broadcasted (brought to memory) to all worker nodes (this avoids shuffling the data of this table).
// MAGIC 
// MAGIC Spark internally maintains a threshold of the table size to automatically apply broadcast joins. The threshold can be configured using “spark.sql.autoBroadcastJoinThreshold”.
// MAGIC 
// MAGIC But you can also hint spark to broadcast a table, using `largedataframe.join(broadcast(smalldataframe), "key")`. Recently Spark has increased the maximum size for the broadcast table from 2GB to 8GB. Thus, it is not possible to broadcast tables which are greater than 8GB.
// MAGIC 
// MAGIC Also be careful here, as broadcasting can also turn against you (happened at Quby!) and cause out of memory errors!
// MAGIC Then you can do: `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)` to disable broadcast joining!
// MAGIC 
// MAGIC Check the execution plan to find out if the join was a broadcast!

// COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

// COMMAND ----------

// let's disable auto broadcast join, so we are in control
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

// COMMAND ----------

import org.apache.spark.sql.functions._

val elecDf = spark.table("electricity_processed")

val tempDf =  spark.table("temperature_processed")

// COMMAND ----------

val joinedDf = tempDf.join(elecDf, Seq("user", "date", "hour"))
display(joinedDf)

// COMMAND ----------

val joinedDf = tempDf.join(broadcast(elecDf), Seq("user", "date", "hour"))
display(joinedDf)

// COMMAND ----------

// MAGIC %md
// MAGIC ## so, is it much faster ?
