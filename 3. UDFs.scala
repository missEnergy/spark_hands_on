// Databricks notebook source
// MAGIC %md
// MAGIC ##date_format()
// MAGIC 
// MAGIC Now, we want to aggregate all the data **by the day of week** (Monday, Tuesday, Wednesday)...
// MAGIC 
// MAGIC ...and then **average the electricity flow per weekday**.
// MAGIC 
// MAGIC Our goal is to see **which day of the week** uses the most energy.
// MAGIC 
// MAGIC On of the functions that can help us with this the operation `date_format(..)` from the `functions` package.

// COMMAND ----------

val df = spark.table("electricity_raw")

// COMMAND ----------

import org.apache.spark.sql.functions._

// Create a new DataFrame and then look at the graph!
display(df
  .withColumn("date", to_date('utc_datetime).cast("String") as "date")
  .withColumn("weekday", date_format('date, "E"))
  .groupBy('weekday)
  .agg(avg('electricity).as("average_electricity"))
  .orderBy('weekday)
)

// what's the matter with the order ?

// COMMAND ----------

// MAGIC %md
// MAGIC What's wrong with this graph?
// MAGIC 
// MAGIC What if we could change the labels from "Mon" to "1-Mon" and "Tue" to "2-Tue"?
// MAGIC 
// MAGIC Would that fix the problem? Sure...
// MAGIC 
// MAGIC Which method would solve that problem...
// MAGIC 
// MAGIC Well there isn't one. We'll just have to create our own...

// COMMAND ----------

// MAGIC %md
// MAGIC ##User Defined Functions (UDF)
// MAGIC 
// MAGIC As you've seen, `DataFrames` can do anything!
// MAGIC 
// MAGIC Actually they can't.
// MAGIC 
// MAGIC There will always be the case for a transformation that cannot be accomplished with the provided functions.
// MAGIC 
// MAGIC To address those cases, Apache Spark provides for **User Defined Functions** or **UDF** for short.
// MAGIC 
// MAGIC However, they come with a cost...
// MAGIC * **UDFs cannot be optimized** by the Catalyst Optimizer
// MAGIC * The function **has to be serialized** and sent out to the executors - this is a one-time cost, but a cost just the same.
// MAGIC * In the case of Python, there is even more over head - Python UDFs for example result in data being serialized between the executor JVM and the Python interpreter running the UDF logic – this significantly reduces performance as compared to UDF implementations in Scala. When you use it in Python, make sure you use vectorized UDFs.
// MAGIC * Another important component of Spark SQL to be aware of is the Catalyst query optimizer. Its capabilities are expanding with every release and can often provide dramatic performance improvements to Spark SQL queries; however, arbitrary UDF implementation code may not be well understood by Catalyst. When there is no built-in replacement, it is still possible to implement and extend Catalyst’s (Spark’s SQL optimizer) expression class.
// MAGIC 
// MAGIC Also, by using built-in Spark SQL functions we cut down our testing effort as everything is performed on Spark’s side. These functions are designed by JVM experts so UDFs are not likely to achieve better performance.
// MAGIC In general, UDF logic should be as lean as possible, given that it will be called for each row.  As an example, a step in the UDF logic taking 100 milliseconds to complete will quickly lead to major performance issues when scaling to 1 billion rows.
// MAGIC 
// MAGIC Let's start with our function...

// COMMAND ----------

def mapDayOfWeek = udf { day:String =>
  // "Mon" --> "1-Mon"
  // "Tue" --> "2-Tue"
  // etc.
}

// COMMAND ----------

// make this cell run!

display(df
  .withColumn("date", to_date('utc_datetime).cast("String") as "date")
  .withColumn("weekday", mapDayOfWeek(date_format('date, "E")))
  .groupBy('weekday)
  .agg(avg('electricity).as("average_electricity"))
  .orderBy('weekday)
)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##I Lied
// MAGIC 
// MAGIC > What API call(s) would solve that problem...<br/>
// MAGIC > Well there isn't one. We'll just have to create our own...
// MAGIC 
// MAGIC The truth is that **there is already a function** to do exactly what we want.
// MAGIC 
// MAGIC Before you go and create your own UDF, check, double check, triple check to make sure it doesn't exist.
// MAGIC 
// MAGIC Remember...
// MAGIC * UDFs induce a performance hit.
// MAGIC * Big ones in the case of Python.
// MAGIC * The Catalyst Optimizer cannot optimize it.
// MAGIC * And you are re-inventing the wheel.
// MAGIC 
// MAGIC In this case, the solution to our problem is the operation `date_format(..)` from the `...sql.functions`.
// MAGIC Can you find the right `date_format` setting and show the weekday graph in a nice way ?

// COMMAND ----------

... answer here

// COMMAND ----------

// MAGIC %md
// MAGIC ##Pandas UDF

// COMMAND ----------

// MAGIC %python 
// MAGIC from pyspark.sql.functions import pandas_udf, PandasUDFType
// MAGIC from pyspark.sql.types import LongType
// MAGIC import random
// MAGIC 
// MAGIC @pandas_udf('double', PandasUDFType.SCALAR)
// MAGIC def random_model(x):
// MAGIC   # imagine you load a python sklearn model here, that you don't want to convert to Scala
// MAGIC   return x + random.random()
// MAGIC 
// MAGIC spark.udf.register('random_model', random_model)

// COMMAND ----------

display(df.withColumn("random", callUDF("random_model", 'electricity)))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##And now you!
// MAGIC 
// MAGIC * Can you make a UDF that for a temperature input returns if is "COLD", "COMFORTABLE" or "HOT" ?
// MAGIC * Given the minimum and maximum temperature occurring in the data, can you make a udf that scales it to a number from 0 one, such that 0 represents the lowest temperature and 1 the highest temperature ?

// COMMAND ----------

// MAGIC %md
// MAGIC ## UDAF
// MAGIC Bonus look up question:
// MAGIC User defined aggregate functions. what is the difference with UDFs ?

// COMMAND ----------


