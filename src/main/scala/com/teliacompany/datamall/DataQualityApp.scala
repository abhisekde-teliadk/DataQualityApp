package com.teliacompany.datamall

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}

object DataQualityApp {
    def main(args: Array[String]) = {
    val spark = SparkSession.builder.appName("DataQualityApp").getOrCreate()
    import spark.implicits._

    input = args(0)
    project = input.split("/")(0)
    name = input.split("/")(1)
    pond = name.split("_")(0)
    root = "/data"
    path = root + "/" + pond + "/" + project + "/" + name + "/" + "data"
    val dataset = spark.read
                       .option("basePath", path)
                       .parquet(path + "/*")

    println("+++ Results")
    dataset.printSchema()
    spark.stop()

    }
}

