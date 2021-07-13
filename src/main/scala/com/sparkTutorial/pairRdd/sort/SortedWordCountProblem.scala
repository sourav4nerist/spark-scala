package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

  /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.DEBUG)
    val conf = new SparkConf().setAppName("sortedWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("in/word_count.text")
    val words = data.flatMap(line => line.split(" ")).map(word => (word, 1))
    val count = words.reduceByKey((x, y) => x + y)
    val countReverse = count.map(wordCount => (wordCount._2, wordCount._1))
    val sortedReverseCount = countReverse.sortByKey(ascending = false)
    val sortedCount = sortedReverseCount.map(elem => elem.swap)

    for ((word, count) <- sortedCount.collect()) println(word + ": " + count)

  }
}

