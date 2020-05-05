// spark-shell -master yarn --conf spark.ui.port=4044
package com.teliacompany.datamall

import com.teliacompany.datamall._
import org.apache.spark.{SparkConf, SparkContext}

import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}

object DataQualityApp {
    def main(args: Array[String]) = {
    val logFile = "DataQuality.log" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()

    /*
        val conf = new SparkConf().setAppName("Hoad HDFS").setMaster("yarn-client")
        val sc = new SparkContext(conf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._
        
        val dataset = sqlContext.read.parquet(args(0))

        val result: VerificationResult = { 
        VerificationSuite()
            .onData(dataset)
            .addCheck(
                Check(CheckLevel.Error, "Data Validation Check")
                    .hasCompleteness("customer_id", _ >= 0.90) // At least 90% rows have customer_id defined
                    .isUnique("review_id")
                    .isNonNegative("total_votes") 
                    .hasStandardDeviation("helpful_votes", _ < 3.0)
                    .hasEntropy("helpful_votes", _ < 2.0)
                    .hasCorrelation("helpful_votes", "total_votes", _ >= 0.8)
                    )
        .run()
        }
        val output = result.checkResults
                           .values
                           .toSeq
                           .toDF("check", "check_level", "check_status", "constraint", "constraint_status", "message")

        println("+++ Results")
        output.show()
        output.write.parquet(args(1)) //, classOf[org.apache.hadoop.io.compress.SnappyCodec])

        println("+++ Results")
        dataset.show()
        sc.stop()
        */
    }
}

