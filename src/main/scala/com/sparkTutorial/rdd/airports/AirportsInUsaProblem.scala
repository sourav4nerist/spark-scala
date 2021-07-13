package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */
    Logger.getLogger("ORG").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airport problem").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val airportsRDD = sc.textFile("in/airports.text")
    airportsRDD.take(10).foreach(line => println(line))
    val airportInUSA = airportsRDD.filter(line => line.split(Utils.COMMA_DELIMITER)(3).equals("\"United States\""))
    val airportAndCity = airportInUSA.map(line => {
      val lineArr = line.split(Utils.COMMA_DELIMITER)
      lineArr(1)+ ", " + lineArr(2)
    }
    )

    airportAndCity.saveAsTextFile("out/practice/airportInUSA.text")

  }
}
