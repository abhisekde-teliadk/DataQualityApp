package com.teliacompany.datamall

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}

object DataQualityApp {
    def main(args: Array[String]) = {
    val spark = SparkSession.builder.appName("DataQualityApp").getOrCreate()
    import spark.implicits._

    val input = args(0)
    val project = input.split("/")(0)
    val name = input.split("/")(1)
    val pond = name.split("_")(0)
    val root = "/data"
    val path = root + "/" + pond + "/" + project + "/" + name + "/" + "data"
    val dataset = spark.read
                       .option("basePath", path)
                       .parquet(path + "/*")

    println("+++ Results")
    dataset.printSchema()
    spark.stop()

    }
}

