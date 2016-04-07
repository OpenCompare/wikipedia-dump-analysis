#!/bin/sh

class="org.opencompare.analysis.WikipediaDumpAnalysisApp"
master="spark://gbecan:7077"
jar="/home/gbecan/git/OpenCompare/wikipedia-dump-analysis/target/wikipedia-dump-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar"

#dump="/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/zu/zu.preprocessed.xml.bz2"
dump="/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/en.preprocessed.xml.bz2"
#language="zu"
language="en"
output="/home/gbecan/git/OpenCompare/wikipedia-dump-analysis/output/"


available_memory=$(free -g | grep -oP '\d+' | head -n 1)
executor_memory=$(( ${available_memory} - 2 ))
echo $available_memory
echo $executor_memory

executor_memory=2

spark-submit --class $class --master $master --conf spark.executor.memory=${executor_memory}g $jar $dump $language $output $@

