// Databricks notebook source
// NOTEBOOK TO PRINT AVERAGE PRICE OF ROOMS
// /FileStore/tables/Property_data.csv


// COMMAND ----------

val data = sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

data.take(2)

// COMMAND ----------

val removeHeader = data.filter(line => !line.contains("Price"))
removeHeader.take(2)

// COMMAND ----------

val roomRdd = removeHeader.map(x => (x.split(",")(3).toInt,x.split(",")(2).toDouble))
roomRdd.take(3)
//takes out only room number and its respective price out as key value pair

// COMMAND ----------

roomRdd.collect()

// COMMAND ----------

val roomRdd = removeHeader.map(x => (x.split(",")(3).toInt,(1,x.split(",")(2).toDouble)))
roomRdd.take(10)

// COMMAND ----------

roomRdd.map((x => x._2)).take(2)

// COMMAND ----------

roomRdd.map((x => x._1)).take(2)

// COMMAND ----------

roomRdd.reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).take(10)
// here x is one complete tuple and y is next complete tuple with same key as of x

// COMMAND ----------

// (4,(1,399000.0)), (4,(1,545000.0)), (4,(1,909000.0)) if this is data then
// x._1 is 4 , y._1 s 4 and y._1 is (1,399000.0) and y._2 is (1,545000.0)

// COMMAND ----------

val reduced_Rdd = roomRdd.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))

// COMMAND ----------

// map values allow you to just work/ make changes on values without doing any change to keys

// COMMAND ----------

val final_Rdd = reduced_Rdd.mapValues(data => data._2/data._1)
final_Rdd.collect()

// COMMAND ----------

for ((bedroom,avg) <- final_Rdd.collect())  println(bedroom + " :" +avg)

// COMMAND ----------

final_Rdd.saveAsTextFile("Property_op.csv")

// COMMAND ----------


