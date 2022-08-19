package com.spark_example;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object Wordcount {
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setJars(Seq("C:/Users/SP Roopa/workspace/test3/target/test3-0.0.1-SNAPSHOT-jar-with-dependencies.jar"));
    val sc = new SparkContext(conf)

    //Read some example file to a test RDD
    val test = sc.textFile("hdfs://localhost:50000/word")

    test.flatMap { line => //for each line
      line.split(" ") //split the line in word by word.
    }
      .map { word => //for each word
        (word, 1) //Return a key/value tuple, with the word as key and 1 as value
      }
      .reduceByKey(_ + _) //Sum all of the value with same key
      .saveAsTextFile("hdfs://localhost:50000/wordoutput2") //Save to a text file

    //Stop the Spark context
    sc.stop
  }
}

