// Databricks notebook source
// for each age find the average and maximum number of friends it has
// /FileStore/tables/FriendsData.csv
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5271821214621819/1847093081170232/5718386297482461/latest.html

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

data.take(2)

// COMMAND ----------

val removeheader = data.filter(line => !line.contains("name"))

// COMMAND ----------

removeheader.take(10)

// COMMAND ----------

val friendrdd = removeheader.map(x => ( x.split(",")(2).toInt, (1,x.split(",")(3).toDouble) ))

// COMMAND ----------

friendrdd.take(5)

// COMMAND ----------

val reducedrdd = friendrdd.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2) )


// COMMAND ----------

reducedrdd.collect()

// COMMAND ----------

val finalrdd = reducedrdd.mapValues( data => data._2 / data._1 )
finalrdd.collect()

// COMMAND ----------

for((age, avg_friends) <- finalrdd.collect() ) println(age + " : " + avg_friends)

// COMMAND ----------

val maxfriendrdd = removeheader.map(x => ( x.split(",")(2).toInt, x.split(",")(3).toDouble ))

maxfriendrdd.take(5)

// COMMAND ----------

val maxrdd = maxfriendrdd.reduceByKey(math.max(_, _))

maxrdd.collect()

// COMMAND ----------

for((age, max_friends) <- maxrdd.collect() ) println(age + " : " + max_friends)

// COMMAND ----------


