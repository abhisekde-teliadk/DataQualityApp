// spark-shell -master yarn --conf spark.ui.port=4044
package com.teliacompany.datamall

import org.apache.spark.{SparkConf, SparkContext}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus

object DataQualityApp {
    def main(args: Array[String]) = {
        val conf = new SparkConf().setAppName("Hoad HDFS").setMaster("yarn-client")
        val sc = new SparkContext(conf)
        sc.stop()
    }
}

