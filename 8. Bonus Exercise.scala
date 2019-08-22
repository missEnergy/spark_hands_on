// Databricks notebook source
// MAGIC %md
// MAGIC ## Let's do something of your interest now!
// MAGIC 
// MAGIC Now it's time to do some free wheeling and apply what we have learned in practice.
// MAGIC 
// MAGIC Feel free to explore the dataset using the transformations and actions you have used so far.
// MAGIC 
// MAGIC When you make queries and/or write results to tables, can you optimize the partitioning?
// MAGIC 
// MAGIC When you make queries, can you use cache, persist or even checkpointing in the right places?
// MAGIC 
// MAGIC Ps. Google is your friend (ow.. and Ellissa of course)
// MAGIC 
// MAGIC ### Inspiration
// MAGIC 
// MAGIC * So far, we haven't looked much at the results of the queries in a visual way. Explore the plot view of the `display` method. Can you come up with some insightful graphs ?
// MAGIC * Windowing in timeseries is quite powerful. Can you compute the duration of each datapoint using `Window` and `lag` ?
// MAGIC * Aggregations such as sum, count, and average are "simple" in distributed computing. Calculating quantiles is less straight forward. Can you find out why? Check out `approxQuantile`!
// MAGIC * The Scala API has a lot of useful syntax and syntactic sugar. In how many ways can you rewrite the simple query `df.filter('user === "6c62cd5594")` ?
// MAGIC * Can you come up with a general partitioning strategy based on the columns of the data. What works well now? What works well if more and more data keeps coming in (like in a production environment) ? What works well if we scale up to more users ?

// COMMAND ----------


