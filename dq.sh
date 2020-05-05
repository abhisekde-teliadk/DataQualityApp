#!/bin/bash
sbt package
echo "Working on ${1}"
spark-submit --class "com.teliacompany.datamall.DataQualityApp" --master yarn --conf spark.ui.port=4444 --jars ./deequ/deequ-1.0.1.jar ./target/scala-2.10/dataquality_2.10-1.0.jar ${1}
