// Databricks notebook source
// /FileStore/tables/numberData.csv
// /FileStore/tables/airports.text
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5271821214621819/491833363668845/5718386297482461/latest.html

// COMMAND ----------

val num_rdd = sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

num_rdd.take(5)

// COMMAND ----------

val header = num_rdd.first
val numdata = num_rdd.filter(line => line!=header)
val dat = numdata.map(x => x.toInt)

// COMMAND ----------

numdata.take(5)

// COMMAND ----------

def prime(num  : Int): Boolean = {
(num > 1) && !(2 to scala.math.sqrt(num).toInt).exists(x =>num % x == 0)
}

// COMMAND ----------

def isPrime2(i :Int) : Boolean = {
     if (i <= 1)
       false
     else if (i == 2)
       true
     else
       !(2 to (i-1)).exists(x => i % x == 0)
   }


// COMMAND ----------

val primes = dat.map(x => (x,prime(x)))

// COMMAND ----------

primes.take(10)

// COMMAND ----------

primes.filter( x => x._2 == true ).map( x => x._1).sum

// COMMAND ----------

val airport_data = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

airport_data.take(2)

// COMMAND ----------

val even_long =data.filter(x=>(x.split(",")(8)).toDouble%2==0).map(x=>x.split(",")(11))
even_long.countByValue

// COMMAND ----------

// longitude greater than 40 or iceland
val long_gt_40 = data.filter(x=>(x.split(",")(7)).toDouble>40 || x.split(",")(3)=="\"Iceland\"")

// COMMAND ----------

long_gt_40.take(10)

// COMMAND ----------


