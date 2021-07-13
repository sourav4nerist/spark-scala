package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SumOfNumbersProblem").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val fileRDD = sc.textFile("in/prime_nums.text")
    val numRDD = fileRDD.flatMap(line => line.split("\\s+")).filter(!_.isEmpty)map(num => num.toInt)
    val sum = numRDD.reduce((a,b) => a+b)
    println(sum)
  }
}
