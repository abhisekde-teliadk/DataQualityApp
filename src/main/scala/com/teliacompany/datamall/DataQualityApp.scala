package com.teliacompany.datamall

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}

object DataQualityApp {
    def main(args: Array[String]) = {
    val spark = SparkSession.builder.appName("DataQualityApp").getOrCreate()
    import spark.implicits._
    
    val dataset = spark.read.parquet(args(0))

    println("+++ Results")
    dataset.show()
    spark.stop()

    }
}

