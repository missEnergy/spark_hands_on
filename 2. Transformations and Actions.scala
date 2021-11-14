// Databricks notebook source
// MAGIC %md
// MAGIC ### A SparkSession object is available out of the box in Databricks

// COMMAND ----------

// val spark = SparkSession.builder().getOrCreate()

spark

// COMMAND ----------

// MAGIC %md
// MAGIC The SparkSession is the entry point for `SparkSQL` and its `DataFrames` and `Datasets` interfaces.
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession

// COMMAND ----------

// let's start by reading in a dataframe
val elecDf = spark.table("electricity_raw")

// COMMAND ----------

// count()
// And counting how many records there are there
elecDf.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### show(..) vs display(..)
// MAGIC Of course we want do see more than just a count of the number of records...
// MAGIC * `show(..)` is part of core spark - `display(..)` is specific to databricks.
// MAGIC * `show(..)` is ugly - `display(..)` is pretty.
// MAGIC * `show(..)` has parameters for truncating both columns and rows - `display(..)` does not.
// MAGIC * `show(..)` is a function of the `DataFrame`/`Dataset` class - `display(..)` works with a number of different objects.
// MAGIC * `display(..)` is more powerful - with it, you can...
// MAGIC   * Download the results as CSV
// MAGIC   * Render line charts, bar chart & other graphs, maps and more.
// MAGIC   * See up to 1000 records at a time.
// MAGIC 
// MAGIC For the most part, the difference between the two is going to come down to preference.
// MAGIC 
// MAGIC Like `DataFrame.show(..)`, `display(..)` is an **action** which triggers a job.

// COMMAND ----------

elecDf.show()

// COMMAND ----------

display(elecDf)

// COMMAND ----------

// MAGIC %md
// MAGIC ##limit(..)
// MAGIC 
// MAGIC Both `show(..)` and `display(..)` are **actions** that trigger jobs
// MAGIC 
// MAGIC `show(..)` has a parameter to control how many records are printed but, `display(..)` does not.
// MAGIC 
// MAGIC We can address that difference with our first transformation, `limit(..)`.
// MAGIC 
// MAGIC `show(..)`, like many actions, does not return anything.
// MAGIC 
// MAGIC On the other hand, transformations like `limit(..)` return a **new** `DataFrame`:

// COMMAND ----------


val limitedDF = elecDf.limit(5) // "limit" the number of records to the first 5

// COMMAND ----------

// MAGIC %md
// MAGIC ### Nothing Happened
// MAGIC * Notice how "nothing" happened - that is no job was triggered.
// MAGIC * This is because we are simply defining the second step in our transformations.
// MAGIC   0. Read in the table.
// MAGIC   0. Limit those records to just the first 5.
// MAGIC * It's not until we induce an action that a job is triggered and the data is processed
// MAGIC 
// MAGIC We can induce a job by calling either the `show(..)` or the `display(..)` actions:

// COMMAND ----------

display(limitedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##select(..)

// COMMAND ----------

display(limitedDF.select("utc_datetime"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##drop(..)
// MAGIC 
// MAGIC As a quick side note, you will quickly discover there are a lot of ways to accomplish the same task.
// MAGIC 
// MAGIC Take the transformation `drop(..)` for example - instead of selecting everything we wanted, `drop(..)` allows us to specify the columns we don't want.
// MAGIC 
// MAGIC And we can see that we can produce the same result as the last exercise this way:

// COMMAND ----------

display(limitedDF.drop("user").drop("electricity"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##distinct() & dropDuplicates()
// MAGIC 
// MAGIC These two transformations do the same thing. In fact, they are aliases for one another.
// MAGIC * You can see this by looking at the source code for these two methods
// MAGIC * ```def distinct(): Dataset[T] = dropDuplicates()```
// MAGIC * See <a href="https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala" target="_blank">Dataset.scala</a>
// MAGIC 
// MAGIC The difference between them has everything to do with the programmer and their perspective.
// MAGIC * The name **distinct** will resonate with developers, analyst and DB admins with a background in SQL.
// MAGIC * The name **dropDuplicates** will resonate with developers that have a background or experience in functional programming.
// MAGIC 
// MAGIC As you become more familiar with the various APIs, you will see this pattern reassert itself.
// MAGIC 
// MAGIC The designers of the API are trying to make the API as approachable as possible for multiple target audiences.

// COMMAND ----------

val distinctDf = elecDf
  .select("user")
  .distinct()

// COMMAND ----------

display(distinctDf)

// COMMAND ----------

distinctDf.count

// COMMAND ----------

// MAGIC %md
// MAGIC ### Exercise
// MAGIC Now load the temperature dataset. How many users occur in this dataset ?

// COMMAND ----------

// MAGIC %md
// MAGIC ##orderBy(..) & sort(..)
// MAGIC 
// MAGIC Both `orderBy(..)` and `sort(..)` arrange all the records in the `DataFrame` as specified.
// MAGIC * Like `distinct()` and `dropDuplicates()`, `sort(..)` and `orderBy(..)` are aliases for each other.
// MAGIC   * `sort(..)` appealing to functional programmers.
// MAGIC   * `orderBy(..)` appealing to developers with an SQL background.
// MAGIC * Like `orderBy(..)` there are two variants of these two methods:
// MAGIC   * `orderBy(Column)`
// MAGIC   * `orderBy(String)`
// MAGIC   * `sort(Column)`
// MAGIC   * `sort(String)`
// MAGIC 
// MAGIC All we need to do now is sort our previous `DataFrame`.

// COMMAND ----------

display(elecDf.orderBy("utc_datetime")) //display(elecDf.orderBy(desc("utc_datetime")))

// COMMAND ----------

import org.apache.spark.sql.functions._

display(elecDf.orderBy(col("utc_datetime")))  //display(elecDf.orderBy(col("utc_datetime").desc))

// COMMAND ----------

display(elecDf.orderBy('utc_datetime)) // display(elecDf.orderBy('utc_datetime.desc))

// COMMAND ----------

// MAGIC %md
// MAGIC ##The Column Class
// MAGIC 
// MAGIC The `Column` class is an object that encompasses more than just the name of the column, but also column-level-transformations, such as sorting in a descending order.
// MAGIC 
// MAGIC The `Column` objects provide us a programmatic way to build up SQL-ish expressions.
// MAGIC 
// MAGIC Besides the `Column.desc()` operation we used above, we have a number of other operations that can be performed on a `Column` object.
// MAGIC 
// MAGIC Here is a preview of the various functions
// MAGIC 
// MAGIC **Column Functions**
// MAGIC * Various mathematical functions such as add, subtract, multiply & divide
// MAGIC * Various bitwise operators such as AND, OR & XOR
// MAGIC * Various null tests such as `isNull()`, `isNotNull()` & `isNaN()`.
// MAGIC * `as(..)`, `alias(..)` & `name(..)` - Returns this column aliased with a new name or names (in the case of expressions that return more than one column, such as explode).
// MAGIC * `between(..)` - A boolean expression that is evaluated to true if the value of this expression is between the given columns.
// MAGIC * `cast(..)` & `astype(..)` - Convert the column into type dataType.
// MAGIC * `asc(..)` - Returns a sort expression based on the ascending order of the given column name.
// MAGIC * `desc(..)` - Returns a sort expression based on the descending order of the given column name.
// MAGIC * `startswith(..)` - String starts with.
// MAGIC * `endswith(..)` - String ends with another string literal.
// MAGIC * `isin(..)` - A boolean expression that is evaluated to true if the value of this expression is contained by the evaluated values of the arguments.
// MAGIC * `like(..)` - SQL like expression
// MAGIC * `rlike(..)` - SQL RLIKE expression (LIKE with Regex).
// MAGIC * `substr(..)` - An expression that returns a substring.
// MAGIC * `when(..)` & `otherwise(..)` - Evaluates a list of conditions and returns one of multiple possible result expressions.
// MAGIC 
// MAGIC The complete list of functions differs from language to language.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC If you look at the API docs, `filter(..)` and `where(..)` are described like this:
// MAGIC > Filters rows using the given condition.
// MAGIC 
// MAGIC Both `filter(..)` and `where(..)` return a new dataset containing only those records for which the specified condition is true.
// MAGIC * Like `distinct()` and `dropDuplicates()`, `filter(..)` and `where(..)` are aliases for each other.
// MAGIC   * `filter(..)` appealing to functional programmers.
// MAGIC   * `where(..)` appealing to developers with an SQL background.
// MAGIC * Like `orderBy(..)` there are two variants of these two methods:
// MAGIC   * `filter(Column)`
// MAGIC   * `filter(String)`
// MAGIC   * `where(Column)`
// MAGIC   * `where(String)`
// MAGIC * Unlike `orderBy(String)` which requires a column name, `filter(String)` and `where(String)` both expect an SQL expression.

// COMMAND ----------

display(elecDf.filter('electricity > 1000))

// COMMAND ----------

display(elecDf.filter('utc_datetime > "2016-11-03"))

// COMMAND ----------

display(elecDf.filter('user === "6c62cd5594"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC **Question:** In most every programming language, what is a single equals sign (=) used for?
// MAGIC 
// MAGIC **Question:** What are two equal signs (==) used for?
// MAGIC 
// MAGIC An important difference for Spark is the return value. For Column:
// MAGIC 
// MAGIC == returns a boolean
// MAGIC 
// MAGIC === returns a column (which contains the result of the comparisons of the elements of two columns)
// MAGIC 
// MAGIC Try it...

// COMMAND ----------

col("user") == "abc"

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC col("user") === "abc"

// COMMAND ----------

val firstRow = elecDf.first()

// COMMAND ----------

// MAGIC %md
// MAGIC ##The Row Class
// MAGIC 
// MAGIC Now that we have a reference to the object backing the first row (or any row), we can use it to extract the data for each column.
// MAGIC 
// MAGIC Before we do, let's take a look at the API docs for the `Row` class.
// MAGIC 
// MAGIC At the heart of it, we are simply going to ask for the value of the object in column N via `Row.get(i)`.
// MAGIC 
// MAGIC Python being a loosely typed language, the return value is of no real consequence.
// MAGIC 
// MAGIC However, Scala is going to return an object of type `Any`. In Java, this would be an object of type `Object`.
// MAGIC 
// MAGIC What we need (at least for Scala), especially if the data type matters in cases of performing mathematical operations on the value, we need to call one of the other methods:
// MAGIC * `getAs[T](i):T`
// MAGIC * `getDate(i):Date`
// MAGIC * `getString(i):String`
// MAGIC * `getInt(i):Int`
// MAGIC * `getLong(i):Long`
// MAGIC 
// MAGIC We can now put it all together to get the number of requests for the most requested project:

// COMMAND ----------

val user = firstRow.getString(2)

print(user)

// What if I ask for the wrong type?
// firstRow.getLong(2)

// COMMAND ----------

// MAGIC %md
// MAGIC ##collect()
// MAGIC 
// MAGIC If you look at the API docs, `collect(..)` is described like this:
// MAGIC > Returns an array that contains all of Rows in this Dataset.
// MAGIC 
// MAGIC `collect()` returns a collection of the specific type backing each record of the `DataFrame`.
// MAGIC * In the case of Python, this is always the `Row` object.
// MAGIC * In the case of Scala, this is also a `Row` object.
// MAGIC 
// MAGIC Building on our last example, let's take the top 10 records and print them out.

// COMMAND ----------

val rows = elecDf
  .limit(10)           // We only want the first 10 records.
  .collect()           // The action returning all records in the DataFrame

// rows is an Array. Now in the driver,
// we can just loop over the array and print 'em out.

rows.foreach( row => {
  val elec = row.getDouble(0)
  print(elec + "\n")
})

// COMMAND ----------

// MAGIC %md
// MAGIC ### Exercise
// MAGIC 
// MAGIC Let's work with the temperature data now.
// MAGIC Let's filter out some outliers! Check if the data contains some unusual high/low temperatures. Can you filter them out ?

// COMMAND ----------

// MAGIC %md
// MAGIC ## withColumnRenamed(..), withColumn(..)
// MAGIC 
// MAGIC How can we add columns and convert the data to the formats we want?
// MAGIC In order to do so, a lot of useful methods are already predefined in the following package!

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// an example

val newDf = elecDf
  .withColumnRenamed("utc_datetime", "datetime")
  .withColumn("date", to_date('datetime).cast("String"))
  .withColumn("hour", hour('datetime))
  .withColumn("electricity", 'electricity.cast("Integer"))

display(newDf)

// COMMAND ----------

newDf.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Try out the cast method on the electricity dataframe. Double is too precise for the electricity flow, so cast it to Integer.
// MAGIC 
// MAGIC And what about adding a column to this dataframe which only contains the date, without time ?

// COMMAND ----------

// MAGIC %md
// MAGIC ##groupBy()
// MAGIC 
// MAGIC This function is a **wide** transformation - it will produce a shuffle and conclude a stage boundary.
// MAGIC 
// MAGIC Unlike all of the other transformations we've seen so far, this transformation does not return a `DataFrame`.
// MAGIC * In Scala it returns `RelationalGroupedDataset`
// MAGIC 
// MAGIC This is because the call `groupBy(..)` is only 1/2 of the transformation.
// MAGIC 
// MAGIC To see the other half, we need to take a look at it's return type, `RelationalGroupedDataset`.

// COMMAND ----------

// MAGIC %md
// MAGIC ### RelationalGroupedDataset
// MAGIC 
// MAGIC If we take a look at the API docs for `RelationalGroupedDataset`, we can see that it supports the following aggregations:
// MAGIC 
// MAGIC | Method | Description |
// MAGIC |--------|-------------|
// MAGIC | `avg(..)` | Compute the mean value for each numeric columns for each group. |
// MAGIC | `count(..)` | Count the number of rows for each group. |
// MAGIC | `sum(..)` | Compute the sum for each numeric columns for each group. |
// MAGIC | `min(..)` | Compute the min value for each numeric column for each group. |
// MAGIC | `max(..)` | Compute the max value for each numeric columns for each group. |
// MAGIC | `mean(..)` | Compute the average value for each numeric columns for each group. |
// MAGIC | `agg(..)` | Compute aggregates by specifying a series of aggregate columns. |
// MAGIC | `pivot(..)` | Pivots a column of the current DataFrame and perform the specified aggregation. |
// MAGIC 
// MAGIC With the exception of `pivot(..)`, each of these functions return our new `DataFrame`.

// COMMAND ----------

display(
  newDf
    .groupBy('user)
    .agg(avg("electricity").as("average_electricity"))
)

// COMMAND ----------

// Exercise: let's pull it all together now: for both electricity and temperature,
// create a dataframe that holds the average value (electricity / temperature) per user, for each hour of each date
// save the dataframes to tables with name "<>_processed"

// COMMAND ----------
//
//// Exercise: the display functionality has a build-in plotting tool
//// Using all the transformations you just learned, what can you show with these datasets ?
//// Try it out!

// COMMAND ----------

// Exercise: org.apache.spark.sql.functions._
// There are so many predefined functions available, it's super helpful!
// Scala docs: https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html
// More readable: https://spark.apache.org/docs/latest/api/sql/index.html
// Try it out!
// Can you convert the utc_datetime in dd/mm/yyyy format?
// Can you convert the utc_datetime to integer timestamp format?
