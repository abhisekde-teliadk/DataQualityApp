package com.teliacompany.datamall

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}

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
    val project = p_items(p_items.indexOf(dataset) -1)
    val out_name = "/data/" + pond + "/checks_" + project + "_" + in_name

    val df = spark.read
                  .option("basePath", path)
                  .parquet(path + "/*")

    val stage1 = suggest_constraints(df)
    val output = apply_checks(df, stage1, spark)
    
    output.show()
    println("+++ Results")    
    output.write.parquet(out_name)
    spark.stop()

    }

    def suggest_constraints(dataset: DataFrame) = {

        val schema = dataset.schema
                            .map(e => (in_name + "." + e.name, e.dataType.typeName))
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
                                        constraint => (in_name, column, constraint.currentValue.split(": ")(0), constraint.currentValue.split(": ")(1))  
                                    } 
                          }
                         .toSeq.toDF("name", "column", "constraint", "current_value")
        // return
        schema.join(sugg, Seq("column", "name"), "inner")
    }

    def apply_checks(dataset: DataFrame, suggestion: DataFrame, session: SparkSession) = {

        val completeness = suggestion.where(suggestion("current_value").startsWith("Completeness"))
        val compliance = suggestion.where(suggestion("constraint").startsWith("Compliance"))

        val check = {
            var init_c = Check(CheckLevel.Error, "Data Validation Check")
            completeness.foreach(c => init_c.hasCompleteness(c(0), _ >= 0.99))
        }

        val result: VerificationResult = { 
        VerificationSuite().onData(dataset)
                           .addCheck(check
                                //Check(CheckLevel.Error, "Data Validation Check")
                                //    .hasCompleteness("customer_id", _ >= 0.90) // At least 90% rows have customer_id defined
                                //    .isUnique("review_id")
                                //    .isNonNegative("total_votes") 
                                //    .hasStandardDeviation("helpful_votes", _ < 3.0)
                                //   .hasEntropy("helpful_votes", _ < 2.0)
                                //    .hasCorrelation("helpful_votes", "total_votes", _ >= 0.8)
                                    )
                           .run()
        }
        // return
        checkResultsAsDataFrame(session, result)
    }
}

