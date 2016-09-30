#!/bin/sh

source ~/.bashrc

class="org.opencompare.analysis.WikipediaDumpAnalysisApp"
jar="/home/gbecan/opencompare/wikipedia-dump-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar"

master=$1
dump=$2
language=$3
output=$4
export_pcm=$5
partitions=$6

#available_memory=$(free -g | grep -oP '\d+' | head -n 1)
#executor_memory=$(( ${available_memory} - 3 ))
executor_memory=6
driver_memory=6

java -version

spark-submit --class $class --master $master \
--conf spark.executor.memory=${executor_memory}g \
--conf spark.executor.cores=1 \
--conf spark.driver.memory=${executor_memory}g \
--conf spark.driver.cores=1 \
--conf spark.driver.maxResultSize=0 \
$jar $dump $language $output $export_pcm $partitions

