// Databricks notebook source
val data = List("tushar","goyal","tushar","sir","khushboo","priya","goyal","tushar")

// COMMAND ----------

val datardd = sc.parallelize(data)

// COMMAND ----------

datardd.count()

// COMMAND ----------

datardd.countByValue()

// COMMAND ----------

val nums = List(1,2,3,4,5)

val nums_rdd = sc.parallelize(nums)

// COMMAND ----------

val productRdd = nums_rdd.reduce((x,y) => x * y)
//reduce finds the product of all elements in nums array
productRdd

// COMMAND ----------


