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
      
      
      val url : String = ""
      val tableName: String = ""
      val userName: String = ""
      val password : String = ""
      val driverName: String = "com.amazon.hive.jdbc41.HS2Driver"
      
      
      val jdbcDF = spark.read
     .format("jdbc")
     .option("url", url)
     .option("dbtable", tableName)
     .option("user", userName)
     .option("password", password)
     .option("driver", driverName)
     .load()
  
    jdbcDF.show
    jdbcDF.printSchema()
   }
   catch{
    case x: Throwable => println("Server Not Responding: " , x)
   }

  }
}
