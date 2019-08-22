// Databricks notebook source
// MAGIC %md
// MAGIC ## How to partition your data for fast querying.

// COMMAND ----------

spark.table("electricity_processed").rdd.getNumPartitions

// COMMAND ----------

spark.table("electricity_processed")
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .partitionBy("user")
  .saveAsTable("electricity_processed_partioned_by_user")

// COMMAND ----------

spark.table("electricity_processed_partioned_by_user").rdd.getNumPartitions

// COMMAND ----------

display(spark.table("electricity_processed").filter('user === "014dd64db3"))

// COMMAND ----------

display(spark.table("electricity_processed_partioned_by_user").filter('user === "014dd64db3"))

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

//check https://stackoverflow.com/questions/40416357/spark-sql-difference-between-df-repartition-and-dataframewriter-partitionby
//so how many files were in 1 partition first ? why is there 1 now ?

// COMMAND ----------

display(spark.table("electricity_processed_partioned_by_user2").filter('user === "014dd64db3"))

// COMMAND ----------

// MAGIC %fs ls /user/hive/warehouse/electricity_processed_partioned_by_user2/user=014dd64db3/

// COMMAND ----------

// MAGIC %md
// MAGIC ##Preparing data for work
// MAGIC So that's it for fast quering.
// MAGIC Next, if we will be working on any given dataset for a while, there are a handful of "necessary" steps to get us ready...
// MAGIC 
// MAGIC **Steps**
// MAGIC 0. Read the data in
// MAGIC 0. Balance the number of partitions to the number of slots
// MAGIC 0. Cache the data
// MAGIC 0. Adjust the `spark.sql.shuffle.partitions`

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
// MAGIC If our goal is to process all our data (say 2M records) in parallel, we need to divide that data up.
// MAGIC 
// MAGIC If I have 8 **slots** for parallel execution, it would stand to reason that I want 2M / 8 or 250,000 records per partition.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC * As we saw at the beginning when reading in `electricity_processed` is **NOT** coincidental that we have **8 slots** and **8 partitions**
// MAGIC * In Spark 2.0 a lot of optimizations have been added to the readers.
// MAGIC * Namely the readers looks at **the number of slots**, the **size of the data**, and makes a best guess at how many partitions **should be created**.
// MAGIC * You can actually double the size of the data several times over and Spark will still read in **only 8 partitions**.
// MAGIC * Eventually it will get so big that Spark will forgo optimization and read it in as 10 partitions, in that case.
// MAGIC 
// MAGIC But 8 partitions and 8 slots is just too easy.
// MAGIC   * Let's read in another copy of this same data.
// MAGIC   * A parquet file that was saved in 5 partitions.
// MAGIC   * This gives us an excuse to reason about the **relationship between slots and partitions**

// COMMAND ----------

spark.table("electricity_raw").repartition(5)
  .write
  .format("parquet")
  .mode(SaveMode.Overwrite)
  .saveAsTable("electricity_raw_5_partitions")

// COMMAND ----------

val alternateDF = spark.table("electricity_raw_5_partitions")

printf("Partitions: %d%n%n", alternateDF.rdd.getNumPartitions)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have only 5 partitions we have to ask...
// MAGIC 
// MAGIC What is going to happen when I perform and action like `count()` **with 8 slots and only 5 partitions?**

// COMMAND ----------

alternateDF.count()

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

val repartitionedDF = alternateDF.repartition(8)

printf("Partitions: %d%n%n", repartitionedDF.rdd.getNumPartitions)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC We just balanced the number of partitions to the number of slots.
// MAGIC 
// MAGIC Depending on the size of the data and the number of partitions, the shuffle operation can be fairly expensive (though necessary).
// MAGIC 
// MAGIC Let's cache the result of the `repartition(n)` call..
// MAGIC * Or more specifically, let's mark it for caching.
// MAGIC * The actual cache will occur later once an action is performed
// MAGIC * Or you could just execute a count to force materialization of the cache.

// COMMAND ----------

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
  .orderBy('electricity) // sort the data
  .foreach(x => ())      // litterally does nothing except trigger a job

// COMMAND ----------

// MAGIC %md
// MAGIC ### Quick Detour
// MAGIC Something isn't right here...
// MAGIC * We only executed one action.
// MAGIC * But two jobs were triggered.
// MAGIC * If we look at the physical plan we can see the reason for the extra job.
// MAGIC * The answer lies in the step **Exchange rangepartitioning**

// COMMAND ----------

// Look at the explain with all records.
repartitionedDF
  .orderBy('electricity)
  .explain()

println("-"*80)

// Look at the explain with only 1M records.
repartitionedDF
  .orderBy('electricity)
  .limit(1000)
  .explain()

println("-"*80)

// COMMAND ----------

repartitionedDF
  .orderBy('electricity) // sort the data
  .limit(1000)           
  .foreach(x => ())               // litterally does nothing except trigger a job

// COMMAND ----------

// MAGIC %md
// MAGIC Only 1 job.
// MAGIC 
// MAGIC Spark's Catalyst Optimizer is optimizing our jobs for us!
// MAGIC 
// MAGIC When you are using the high-level dataframe/dataset APIs, you leave it up to Spark to determine the execution plan, including the job/stage chunking. These depend on many factors such as execution parallelism, cached/persisted data structures, etc. You may see more jobs per query as, for example, some data sources are sampled to parameterize cost-based execution optimization.

// COMMAND ----------

// MAGIC %md
// MAGIC ### The Real Problem
// MAGIC 
// MAGIC Back to the original issue...
// MAGIC * Rerun the original job (below).
// MAGIC * Take a look at the second job.
// MAGIC * Look at the 3rd Stage.
// MAGIC * Notice that it has 200 partitions!
// MAGIC * And this is our problem.

// COMMAND ----------


val funkyDF = repartitionedDF
  .orderBy('electricity) // sorts the data

funkyDF.foreach(x => ())          // litterally does nothing except trigger a job

// COMMAND ----------

// MAGIC %md
// MAGIC The problem is the number of partitions we ended up with.
// MAGIC 
// MAGIC Besides looking at the number of tasks in the final stage, we can simply print out the number of partitions

// COMMAND ----------


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
  .orderBy('electricity) // sort the data
//
betterDF.foreach(x => () )        // litterally does nothing except trigger a job

printf("Partitions: %,d%n", betterDF.rdd.getNumPartitions)

// COMMAND ----------


