// Databricks notebook source
var a:Int = 1

var b = 1:Int


a = 10

// COMMAND ----------

val c = 100


c=10

// COMMAND ----------

// lazy val  
// lazy mean -> if you apply lazy 

lazy val a = 10
  
a

// COMMAND ----------

lazy val variable_lazy = { println("hello wordl");5 }

variable_lazy

// COMMAND ----------

variable_lazy

// COMMAND ----------

lazy val sum = 10 + b

val b =9

println(sum)

// COMMAND ----------

lazy var a=10

// COMMAND ----------

// more indepth of variable declaration

val `my name is tushar` = "goyal"

// COMMAND ----------

val `val` = 10

print(`val`)

// COMMAND ----------

// string


val a = "\t\n\u03BB"

// COMMAND ----------

def square(a:Int): Int ={
  
  a*a
}

square(2)

// public static void main

// COMMAND ----------

def add(a:Int, b:Int) = a+b

println(add(6,7))

// COMMAND ----------

val `void` = 100


val `false` = true

val `return`= 90

if(`false`) `void` else `return`

// COMMAND ----------

def square(a: Int):Int = {
  a*a
}


def  sq2(y:Int, takeFunction: Int => Int ): Int= {   // takefunction -> Square
  
  takeFunction(y)                    // square will be call here (argument y)  -> square(2)
}



sq2(2, square)

// COMMAND ----------

// collections:

val new1 = List(1,2,3,4,5,6,7)

new1

// COMMAND ----------

// data access as per index position
new1(6)

// COMMAND ----------

new1.reverse

// COMMAND ----------

new1.head

// COMMAND ----------

new1.tail

// COMMAND ----------

// replace some data

new1(0) = 100

// COMMAND ----------

// array

var new2  = Array(1,2,3,4,5)

// COMMAND ----------

new2(0) = 100

new2

// COMMAND ----------

// DBTITLE 1,First Rdd - SC (spark Context  -> Object databricks which is used to work with cluster)
// first rdd
//  parallelize method


val data = List(1,2,3,4,5)

//  parallize method used to create rdd & is lazy in nature
val creationRDD = sc.parallelize(data)

// COMMAND ----------

creationRDD

// COMMAND ----------

//  to get result of rdd -> action on your rdd

creationRDD.collect()

// COMMAND ----------

// get the total partitions for your data
creationRDD.partitions.size

// COMMAND ----------

val rddParttition = sc.parallelize(List(1,2,3,4), 2  )





// COMMAND ----------

rddParttition.partitions.size

// COMMAND ----------

// count is also action - return the number of element

rddParttition.count()

// COMMAND ----------

//  map -> transformation

// we are using map to create new rdd from an exisiting rdd [rddPartitions]
val maprdd = rddParttition.map( x => x * x* x )


// maprdd.take(3)

maprdd.collect()

// COMMAND ----------

maprdd.filter( x => x % 2 ==0 ).collect()

// COMMAND ----------

val mainrdd = sc.parallelize( List("hey","hello","tushar","sir") )

mainrdd.collect()

// COMMAND ----------

//  map vs flatmap

mainrdd.map( x => x.split(",")).collect()


// COMMAND ----------

mainrdd.flatMap(x => x.split(",")).collect()

// COMMAND ----------

// creating rdd for key value pair

val rdd0 = sc.parallelize(Array("one","two","three","one","two"))

// COMMAND ----------

val keyrdd = rdd0.map( x => (x,5))


keyrdd.collect()

// COMMAND ----------

keyrdd.reduceByKey(_+_).collect()
