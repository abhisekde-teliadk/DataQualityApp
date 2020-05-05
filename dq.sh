#!/bin/bash
export HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH/etc/hadoop
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
sbt package
spark2-submit --class "com.teliacompany.datamall.DataQualityApp" --master yarn --conf spark.ui.port=4444 ./target/scala-2.11/dataquality_2.11-1.0.jar ${*}
