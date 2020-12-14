// Databricks notebook source
 //File uploaded to /FileStore/tables/nasa_august.tsv
 //File uploaded to /FileStore/tables/nasa_july.tsv

// COMMAND ----------

val july_rdd = sc.textFile("/FileStore/tables/nasa_july.tsv")
val aug_rdd = sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val union_rdd = july_rdd.union(aug_rdd)

union_rdd.take(3)

// COMMAND ----------

val header = union_rdd.first

// COMMAND ----------

union_rdd.filter(line => line!=header).take(2)

// COMMAND ----------

// DBTITLE 1,2nd method to remove header and show rows
def header_remover(line:String): Boolean = !(line.startsWith("host"))

// COMMAND ----------

// DBTITLE 1,because line in header starts with 'host' string
union_rdd.filter(x => header_remover(x)).take(2)

// COMMAND ----------

val rdd_no_head = union_rdd.filter(line => line!=header)

// COMMAND ----------

val rdd_response = rdd_no_head.map(x=>x.split("\t")(5))
rdd_response.take(2)

// COMMAND ----------

val rdd_zero_response =  rdd_response.filter(line => line.toInt >0)
rdd_zero_response.take(10)

// COMMAND ----------

rdd_zero_response.count()

// COMMAND ----------

rdd_no_head.sample(withReplacement = true, fraction= 0.2).collect()

// COMMAND ----------

// to do intersection of both files first column x!=String(host)

// COMMAND ----------


