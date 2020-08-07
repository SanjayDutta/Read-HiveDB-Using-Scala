package com.sanjay.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    
    Logger.getLogger("org").setLevel(Level.ERROR)
   
   val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[*]")

   try{
     val sc = new SparkContext(conf)
      val spark = SparkSession.builder().appName("Spark Hive Example").getOrCreate()
      
      val jdbcDF = spark.read
     .format("jdbc")
     .option("url", "jdbc:hive2://52.66.218.85:10000/FIRSTDB")
     .option("dbtable", "secondTABLE ")
     .option("user", "hive ")
     .option("password", "qfbmD3ggd5LdTJ1z")
     .option("driver", "com.amazon.hive.jdbc41.HS2Driver")
     .load()
  
    jdbcDF.show
    jdbcDF.printSchema()
   }
   catch{
    case x: Throwable => println("Server Not Responding: " , x)
   }

  }
}
