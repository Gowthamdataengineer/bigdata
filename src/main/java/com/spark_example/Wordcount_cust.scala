//package spark

package com.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Wordcount_cust {
 def main(args: Array[String]) {
 
 //Create conf object
 val conf = new SparkConf()
 .setAppName("WordCount")
 .setMaster("local")
 //create spark context object
 val sc = new SparkContext(conf)

//Check whether sufficient params are supplied
 
 //Read file and create RDD
 val rawData = sc.textFile("/home/big/word.txt",2)

 //convert the lines into words using flatMap operation
 val words = rawData.flatMap(line => line.split(" "))


 //count the individual words using map and reduceByKey operation
 val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)
 

 //val partitionedData=wordCount.partitionBy(new MyCustomerPartitioner(2))

 //Save the result
 wordCount.saveAsTextFile("output_withoutpartition")

//stop the spark context
 sc.stop
 }
}

class MyCustomerPartitioner(numParts: Int) extends Partitioner {
 override def numPartitions: Int = numParts
 
 override def getPartition(key: Any): Int = 
 {
    if( key.toString().equalsIgnoreCase("welcome"))
       return 0
   else (key.toString().equalsIgnoreCase("hi")&& key.toString().equalsIgnoreCase("hello"))
      return 1
 }
}
