package com.sparkTutorial.pairRdd.filter

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsNotInUsaSolution {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("airports").setMaster("local")
    val sc = new SparkContext(conf)

    val airportsRDD = sc.textFile("in/airports.text")

    val airportPairRDD = airportsRDD.map(line => (line.split(Utils.COMMA_DELIMITER)(1),
      line.split(Utils.COMMA_DELIMITER)(3)))
    val airportsNotInUSA = airportPairRDD.filter(keyValue => keyValue._2 != "\"United States\"")
   // val airportNotInUSA = airportPairRDD.filter((a,b) => b != "\"United States\"")
    airportsNotInUSA.saveAsTextFile("out/existingCode/airports_not_in_usa_pair_rdd.text")
  }
}
