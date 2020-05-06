package com.teliacompany.datamall

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.checks.Check.haveCompleteness

object DataQualityApp {
    def main(args: Array[String]) = {
    val spark = SparkSession.builder.appName("DataQualityApp").getOrCreate()
    import spark.implicits._

    if (args.length == 0)
        throw new Exception("No dataset provided. Please pass a fully qualified path of a dataset as argument.")

    val path = args(0)
    val p_items = path.split("/")
    val pond = p_items(p_items.indexOf("data") +1)
    val in_name = p_items(p_items.lastIndexOf("data") -1)
    val project = p_items(p_items.indexOf(in_name) -1)
    val out_name = "/data/" + pond + "/checks_" + project + "_" + in_name

    val df = spark.read
                  .option("basePath", path)
                  .parquet(path + "/*")

    val stage1 = suggest_constraints(in_name, df, spark)
    val output = apply_checks(in_name, df, stage1, spark)
    
    // Output
    output.show()
    println("+++ Results")    
    output.write
          .mode("append")
          .parquet(out_name)
    spark.stop()

    }

    def suggest_constraints(name: String, dataset: DataFrame, spark: SparkSession) = {
        import spark.implicits._

        val schema = dataset.schema
                            .map(e => (name, e.name, e.dataType.typeName))
                            .toDF("name", "column", "data_type")

        val result = { 
        ConstraintSuggestionRunner()
            .onData(dataset)
            .addConstraintRules(Rules.DEFAULT) 
            .run()
        }

        val sug1 = result.constraintSuggestions
                         .flatMap { 
                                    case (column, suggestions) =>  suggestions.map { 
                                        constraint => (name, column, constraint.currentValue.split(": ")(0), constraint.currentValue.split(": ")(1))  
                                    } 
                          }
                         .toSeq.toDF("name", "column", "constraint", "current_value")
        // return
        schema.join(sug1, Seq("column", "name"), "inner")
    }

    def apply_checks(name: String, dataset: DataFrame, suggestion: DataFrame, session: SparkSession) = {

        val completeness = suggestion.where(suggestion("current_value").startsWith("Completeness"))
        val compliance = suggestion.where(suggestion("constraint").startsWith("Compliance"))

        
        val col_list = completeness.select("column")
                        .collect
                        .map(e => e(0).toString)
                        .toSeq

        var checks = Check(CheckLevel.Error, "Data Validation Check").haveCompleteness(col_list, _ >= 0.99) // 99% rows of each columns are populated
        
        val result: VerificationResult = { 
            VerificationSuite().onData(dataset)
                .addCheck(checks)
                .run()
        }
        // return
        checkResultsAsDataFrame(session, result)
    }
}

