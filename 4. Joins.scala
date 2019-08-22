// Databricks notebook source
// MAGIC %md
// MAGIC ##join(..)
// MAGIC 
// MAGIC If you are familiar with SQL joins then `DataFrame.join(..)` should be pretty strait forward.
// MAGIC 
// MAGIC We start with the left side - in this example the hourly temperature data you've created.
// MAGIC 
// MAGIC Then join the right side - in this example the hourly electricity data you've created.

// COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

import org.apache.spark.sql.functions._

val elecDf = spark.table("electricity_processed")

val tempDf = spark.table("temperature_processed")


// // or use these...
// val elecDf = spark.table("electricity_raw")
//    .withColumnRenamed("utc_datetime", "datetime")
//    .withColumn("date", to_date('datetime).cast("String") as "date")
//    .withColumn("hour", hour('datetime) as "hour")
//    .groupBy('user, 'date, 'hour)
//    .agg(avg("electricity").as("average_electricity"))
//
// val tempDf = spark.table("temperature_raw")
//    .withColumnRenamed("utc_datetime", "datetime")
//    .withColumn("date", to_date('datetime).cast("String") as "date")
//    .withColumn("hour", hour('datetime) as "hour")
//    .groupBy('user, 'date, 'hour)
//    .agg(avg("inside").as("average_temperature"))

// COMMAND ----------

display(elecDf)

// COMMAND ----------

display(tempDf)

// COMMAND ----------

val joinedDf = tempDf.join(elecDf, Seq("user", "date", "hour"))
display(joinedDf)

// COMMAND ----------

// MAGIC %md
// MAGIC Joins in general are expensive since they require that corresponding keys from each dataframe are located at the same partition so that they can be combined locally.
// MAGIC 
// MAGIC If they do not have known partitioners, they will need to be shuffled so that both share a partitioner, and data with the same keys, lives in the same partitions.
// MAGIC 
// MAGIC The join operations can we wide or narrow depending on the situation!

// COMMAND ----------

// MAGIC %md
// MAGIC Fortunately, when you combine joins with other transformations, Spark SQL takes care of most of the query optimization for you! Catalyst can push down or reorder operations to make your joins more efficient!
// MAGIC You do give up some control do, so you can't avoid shuffles of the data.

// COMMAND ----------

// MAGIC %md
// MAGIC ## All the join types
// MAGIC The default join operation is a inner join, so will only include values for keys present in both RDDs, and in the case of multiple values per key, provides all permutations of the key/value pair.
// MAGIC The best scenario for a standard join is when both RDDs contain the same set of distinct keys. With duplicate keys, the size of the data may expand dramatically causing performance issues, and if one key is not present in both RDDs you will lose that row of data. Here are a few guidelines:
// MAGIC * When both RDDs have duplicate keys, the join can cause the size of the data to expand dramatically. It may be better to perform a distinct or operation to reduce the key space instead of producing the full cross product.
// MAGIC * If keys are not present in both RDDs you risk losing your data unexpectedly. It can be safer to use an outer join, so that you are guaranteed to keep all the data in either the left or the right dataframe, then filter the data after the join.
// MAGIC 
// MAGIC Furthermore, the standard SQL join types are all supported and can be specified as the joinType in `df.join(otherDf, sqlCondition, joinType)`.
// MAGIC Spark’s supported join types are “inner,” “left_outer” (aliased as “outer”), “left_anti,” “right_outer,” “full_outer,” and “left_semi.”3 With the exception of “left_semi” these join types all join the two tables, but they behave differently when handling rows that do not have keys in both tables.
// MAGIC 
// MAGIC https://www.oreilly.com/library/view/high-performance-spark/9781491943199/ch04.html gives a nice overview of what all join types do.

// COMMAND ----------

val nameDf = Seq(
  ("baaa89965e", "Alice"),
  ("014dd64db3", "Bob"),
  ("14210e75b9", "Clarice")
).toDF("user", "name")

// COMMAND ----------

// Exercise, make an example with all join types using the nameDf
// It's even easier to see if you first to a distinct call on joinedDf
// Can you explain the results ?
