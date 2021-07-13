package com.sparkTutorial

import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

object Dummy {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]","Dummy")
    val a = Seq((1, "a"), (2, "b"), (1, "c"), (1, "d"), (2, "a"))
    val rdd = sc.parallelize(a)
    val join = rdd.join(rdd)
    join.foreach(println)

    val text = "ab cd 1.a 2.c -sa"
    val rdd1 = sc.parallelize(text.split("\\s"))
    val filterRdd1 = rdd1.filter(t => t.contains(".") || t.contains("-"))
    val filterRdd2 = rdd1.subtract(filterRdd1)
    val spaceRdd = filterRdd2.map(t => " " + t)
    val linedRdd = filterRdd1.map(t => "\n" + t)
    println(spaceRdd.union(linedRdd).collect().mkString.trim)

  }
}
