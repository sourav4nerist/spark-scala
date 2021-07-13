package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    val conf = new SparkConf().setAppName("airport latitude").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val airportsRawRDD = sc.textFile("in/airports.text")
    val airportsWithLatitudeOver40 = airportsRawRDD.filter(line => {
      //line.split(Utils.COMMA_DELIMITER)(6).toFloat>40
      val lat = line.split(Utils.COMMA_DELIMITER)(6)
      !lat.isEmpty && lat.matches("[0-9]+[.]?[0-9]+") && lat.toFloat > 40
    })

    val airportWithLatitude = airportsWithLatitudeOver40.map(line => {
      val lineArr = line.split(Utils.COMMA_DELIMITER)
      lineArr(1) + ", " + lineArr(6)
    })

    airportWithLatitude.saveAsTextFile("out/practice/airports_by_latitude.text")
  }
}
