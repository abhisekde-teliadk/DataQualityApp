package com.teliacompany.datamall

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}
import spark.implicits._

object DataQualityApp {
    def main(args: Array[String]) = {
    val spark = SparkSession.builder.appName("DataQualityApp").getOrCreate()
        
    val dataset = spark.read.parquet(args(0))

    println("+++ Results")
    dataset.show()
    spark.stop()

    }
}

