// spark-shell -master yarn --conf spark.ui.port=4044
package com.teliacompany.datamall

import com.teliacompany.datamall._
import org.apache.spark.{SparkConf, SparkContext}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus

object DataQualityApp {
    def main(args: Array[String]) = {
        val conf = new SparkConf().setAppName("Hoad HDFS").setMaster("yarn-client")
        val sc = new SparkContext(conf)
        load(args(0), sc)
        sc.stop()
    }

    def load(path: String) = {
        val ls = HdfsUtils.getAllFiles(path)
        println("+++ Files list: ")
        println(ls)
    }
}

