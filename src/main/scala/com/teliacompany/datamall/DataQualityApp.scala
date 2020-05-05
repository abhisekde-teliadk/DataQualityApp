package com.teliacompany.datamall

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}

import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}

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

    val result: VerificationResult = { 
        VerificationSuite().onData(work_amz_rev)
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
    val output = checkResultsAsDataFrame(session, result)
    output.show()
    println("+++ Results")
    val out_name = root + "/" + pond + "/checks_" project + "_" + name
    output.write.parquet(out_name)
    spark.stop()

    }
}

