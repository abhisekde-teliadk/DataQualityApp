package com.teliacompany.datamall

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

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

    val result = { 
    ConstraintSuggestionRunner()
        .onData(dataset)
        .addConstraintRules(Rules.DEFAULT) 
        .run()
    }

    val output = result.constraintSuggestions
                       .flatMap { 
                                    case (column, suggestions) =>  suggestions.map { 
                                        constraint => (column, constraint.description, constraint.codeForConstraint)  
                                    } 
                        }.toSeq.toDF("column", "suggestion", "code_snippet")
    output.show()
    println("+++ Results")
    val out_name = root + "/" + pond + "/checks_" + project + "_" + name    
    output.write.parquet(out_name)
    spark.stop()

    }
}

