// Databricks notebook source
// MAGIC %md
// MAGIC ## Partition pruning: How to partition your data for fast querying.
// MAGIC 
// MAGIC If you're data is partitioned on the key that you want to filter on later, you can speed up queries a lot!

// COMMAND ----------

// If you don't have electricity_processed run this cell
import org.apache.spark.sql.functions._

spark.table("electricity_raw")
  .withColumnRenamed("utc_datetime", "datetime")
  .withColumn("date", to_date('datetime).cast("String") as "date")
  .withColumn("hour", hour('datetime) as "hour")
  .groupBy('user, 'date, 'hour)
  .agg(avg("electricity").as("average_electricity"))
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .saveAsTable("electricity_processed")

// COMMAND ----------

// Our table is not yet partitioned by any key
// let's partition it

spark.table("electricity_processed")
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .partitionBy("user")
  .saveAsTable("electricity_processed_partioned_by_user")

// COMMAND ----------

// and compare performance

display(spark.table("electricity_processed").filter('user === "014dd64db3"))

// COMMAND ----------

display(spark.table("electricity_processed_partioned_by_user").filter('user === "014dd64db3"))

// COMMAND ----------

// and what can we see under the hood? how is the data stored?

spark.table("electricity_processed_partioned_by_user").rdd.getNumPartitions

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed_partioned_by_user

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed_partioned_by_user/user=014dd64db3/

// COMMAND ----------

// Question: Why do we need repartition AND partitionBy?
spark.table("electricity_processed").repartition('user)
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .partitionBy("user")
  .saveAsTable("electricity_processed_partioned_by_user2")

// COMMAND ----------

display(spark.table("electricity_processed_partioned_by_user2").filter('user === "014dd64db3"))

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed_partioned_by_user2/

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed_partioned_by_user2/user=014dd64db3/

// COMMAND ----------

spark.table("electricity_processed_partioned_by_user2").rdd.getNumPartitions

// COMMAND ----------

// check https://stackoverflow.com/questions/40416357/spark-sql-difference-between-df-repartition-and-dataframewriter-partitionby
// so how many files were in a partition folder first ? why is there 1 now ?

// COMMAND ----------

// MAGIC %md
// MAGIC ##Partitions vs Slots
// MAGIC 
// MAGIC * For **Step #2** we have to ask the question, what is the relationship between partitions and slots.
// MAGIC 
// MAGIC 
// MAGIC ** *Note:* ** *The Spark API uses the term **core** meaning a thread available for parallel execution.*<br/>*Here we refer to it as **slot** to avoid confusion with the number of cores in the underlying CPU(s)*<br/>*to which there isn't necessarily an equal number.*

// COMMAND ----------

// MAGIC %md
// MAGIC ### Slots/Cores
// MAGIC 
// MAGIC In most cases, if you created your cluster, you should know how many cores you have.
// MAGIC 
// MAGIC However, to check programatically, you can use `SparkContext.defaultParallelism`
// MAGIC 
// MAGIC For more information, see the doc <a href="https://spark.apache.org/docs/latest/configuration.html#execution-behavior" target="_blank">Spark Configuration, Execution Behavior</a>

// COMMAND ----------

// # of cores/slots
sc.defaultParallelism

// COMMAND ----------

// MAGIC %md
// MAGIC ### Partitions
// MAGIC 
// MAGIC * The second 1/2 of this question is how many partitions of data do I have? And why that many?
// MAGIC 
// MAGIC If our goal is to process all our data in parallel, we need to divide that data up.
// MAGIC 
// MAGIC If I have X **slots** for parallel execution, it would stand to reason that I want the data records evenly in X

// COMMAND ----------

// MAGIC %md
// MAGIC What if we had 8 slots of parallelism. And suppose we had a dataframe with only 5 partitions.
// MAGIC 
// MAGIC What is going to happen when I perform and action like `count()` **with 8 slots and only 5 partitions?**

// COMMAND ----------

// MAGIC %md
// MAGIC ### Use Every Slot/Core
// MAGIC 
// MAGIC With some very few exceptions, you always want the number of partitions to be **a factor of the number of slots**.
// MAGIC 
// MAGIC That way **every slot is used**.
// MAGIC 
// MAGIC That is, every slots is being assigned a task.
// MAGIC 
// MAGIC With 5 partitions & 8 slots we are **under-utilizing three of the eight slots**.
// MAGIC 
// MAGIC With 9 partitions & 8 slots we just guaranteed our **job will take 2x** as long as it may need to.
// MAGIC * 10 seconds, for example, to process the first 8.
// MAGIC * Then as soon as one of the first 8 is done, another 10 seconds to process the last partition.

// COMMAND ----------

// MAGIC %md
// MAGIC ##repartition(n) or coalesce(n)
// MAGIC The goal will **ALWAYS** be to use as few partitions as possible while maintaining at least 1 x number-of-slots.
// MAGIC We have two operations that can help address this problem: `repartition(n)` and `coalesce(n)`.
// MAGIC 
// MAGIC If you look at the API docs, `coalesce(n)` is described like this:
// MAGIC > Returns a new Dataset that has exactly numPartitions partitions, when fewer partitions are requested.<br/>
// MAGIC > If a larger number of partitions is requested, it will stay at the current number of partitions.
// MAGIC 
// MAGIC If you look at the API docs, `repartition(n)` is described like this:
// MAGIC > Returns a new Dataset that has exactly numPartitions partitions.
// MAGIC 
// MAGIC The key differences between the two are
// MAGIC * `coalesce(n)` is a **narrow** transformation and can only be used to reduce the number of partitions.
// MAGIC * `repartition(n)` is a **wide** transformation and can be used to reduce or increase the number of partitions.
// MAGIC 
// MAGIC So, if I'm increasing the number of partitions I have only one choice: `repartition(n)`
// MAGIC 
// MAGIC If I'm reducing the number of partitions I can use either one, so how do I decide?
// MAGIC * First off, `coalesce(n)` is a **narrow** transformation and performs better because it avoids a shuffle.
// MAGIC * However, `coalesce(n)` cannot guarantee even **distribution of records** across all partitions.
// MAGIC * For example, with `coalesce(n)` you might end up with **a few partitions containing 80%** of all the data.
// MAGIC * On the other hand, `repartition(n)` will give us a relatively **uniform distribution**.
// MAGIC * And `repartition(n)` is a **wide** transformation meaning we have the added cost of a **shuffle operation**.
// MAGIC 
// MAGIC In our case, we "need" to go from 5 partitions up to 8 partitions - our only option here is `repartition(n)`.

// COMMAND ----------

val repartitionedDF = spark.table("electricity_processed_partioned_by_user2").repartition(8)

printf("Partitions: %d%n%n", repartitionedDF.rdd.getNumPartitions)

// COMMAND ----------

val repartitionedDF = spark.table("electricity_processed_partioned_by_user2").repartition(8).coalesce(5)

printf("Partitions: %d%n%n", repartitionedDF.rdd.getNumPartitions)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Depending on the size of the data and the number of partitions, the shuffle operation can be fairly expensive (though necessary).
// MAGIC 
// MAGIC Let's cache the result of the `repartition(n)` call..
// MAGIC * Or more specifically, let's mark it for caching.
// MAGIC * The actual cache will occur later once an action is performed
// MAGIC * Or you could just execute a count to force materialization of the cache.

// COMMAND ----------

val repartitionedDF = spark.table("electricity_processed_partioned_by_user2").repartition(8)

repartitionedDF.cache()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC The next problem has to do with a side effect of certain **wide** transformations.
// MAGIC 
// MAGIC So far, we haven't hit any **wide** transformations other than `repartition(n)`
// MAGIC * But eventually we will...
// MAGIC * Let's illustrate the problem that we will **eventually** hit
// MAGIC * We can do this by simply sorting our data.

// COMMAND ----------

repartitionedDF
  .orderBy('average_electricity) // sort the data
  .foreach(x => ())      // litterally does nothing except trigger a job

// COMMAND ----------

// MAGIC %md
// MAGIC ### The Problem
// MAGIC 
// MAGIC Back to the original issue...
// MAGIC * Take a look at the second job.
// MAGIC * Look at the 3rd Stage.
// MAGIC * Notice that it has 200 partitions!

// COMMAND ----------

// MAGIC %md
// MAGIC The problem is the number of partitions we ended up with.
// MAGIC 
// MAGIC Besides looking at the number of tasks in the final stage, we can simply print out the number of partitions

// COMMAND ----------

val funkyDF = repartitionedDF
  .orderBy('average_electricity) // sort the data

printf("Partitions: %,d%n", funkyDF.rdd.getNumPartitions)

// COMMAND ----------

// MAGIC %md
// MAGIC The engineers building Apache Spark chose a default value, 200, for the new partition size.
// MAGIC 
// MAGIC After all our work to determine the right number of partitions they go and undo it on us.
// MAGIC 
// MAGIC The value 200 is actually based on practical experience, attempting to account for the most common scenarios to date.
// MAGIC 
// MAGIC Work is being done to intelligently determine this new value but that is still in progress.
// MAGIC 
// MAGIC For now, we can tweak it with the configuration value `spark.sql.shuffle.partitions`
// MAGIC 
// MAGIC We can see below that it is actually configured for 200 partitions

// COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

// MAGIC %md
// MAGIC We can change the config setting with the following command

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

// COMMAND ----------

// MAGIC %md
// MAGIC Now, if we re-run our query, we will see that we end up with the 8 partitions we want post-shuffle.

// COMMAND ----------

val betterDF = repartitionedDF
  .orderBy('average_electricity) // sort the data
//
betterDF.foreach(x => () )        // litterally does nothing except trigger a job

printf("Partitions: %,d%n", betterDF.rdd.getNumPartitions)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## The Quby incident
// MAGIC * Sorting (within a partition) matters for the parquet compression!
// MAGIC * What happens to the file size before and after sorting!
// MAGIC * This can have a big impact for big data storing

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed/

// COMMAND ----------

spark.table("electricity_processed")
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .saveAsTable("electricity_processed_2")

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed_2/

// COMMAND ----------

spark.table("electricity_processed")
  .sort('user)
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .saveAsTable("electricity_processed_sorted")

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed_sorted/

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Predicate push down
// MAGIC Predicate push down is another feature of Spark and Parquet that can improve query performance by reducing the amount of data read from Parquet files. Predicate push down works by evaluating filtering predicates in the query against metadata stored in the Parquet files. Parquet can optionally store statistics (in particular the minimum and maximum value for a column chunk) in the relevant metadata section of its files and can use that information to take decisions, for example, to skip reading chunks of data if the provided filter predicate value in the query is outside the range of values stored for a given column. This is a simplified explanation, there are many more details and exceptions that it does not catch, but it should give you a gist of what is happening under the hood. You will find more details later in this section and further in this post in the paragraph discussing Parquet internals.

// COMMAND ----------

spark.table("electricity_processed")
  .sort('average_electricity)
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .saveAsTable("electricity_processed_sorted_by_elec")

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed_sorted_by_elec/

// COMMAND ----------

display(spark.read.parquet("/user/hive/warehouse/electricity_processed_sorted_by_elec/part-00006-tid-5924591950017092414-412aa338-e0f2-47d6-80b6-47d45a250a68-1572-1-c000.snappy.parquet").agg(max('average_electricity)))

// COMMAND ----------

spark.table("electricity_processed_2").count

// COMMAND ----------

spark.table("electricity_processed_sorted_by_elec").count

// COMMAND ----------

spark.table("electricity_processed_2").filter('average_electricity > 4300).count

// COMMAND ----------

spark.table("electricity_processed_sorted_by_elec").filter('average_electricity > 4300).count

// COMMAND ----------

// MAGIC %md
// MAGIC The run time is not very deterministic, run a couple of times to see the general picture...
// MAGIC The difference is small but you get the idea
// MAGIC https://blog.usejournal.com/sorting-and-parquet-3a382893cde5

// COMMAND ----------


