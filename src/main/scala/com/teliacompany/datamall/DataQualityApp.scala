package com.teliacompany.datamall

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import com.teliacompany.datamall._

import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}

object DataQualityApp {
    def main(args: Array[String]) = {
    val spark = SparkSession.builder.appName("DataQualityApp").getOrCreate()

    // DataFrame and toDF support
    import org.apache.spark.sql.DataFrame
    val session = sqlContext.sparkSession
    import session.sqlContext.implicits._
        
    val dataset = sqlContext.read.parquet(args(0))

    println("+++ Results")
    dataset.show()
    sc.stop()
    
    }
}

