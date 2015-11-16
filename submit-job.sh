#!/bin/sh

class="org.opencompare.analysis.WikipediaDumpAnalysisApp"
master="spark://gbecan:7077"
jar="/home/gbecan/git/OpenCompare/wikipedia-dump-analysis/target/wikipedia-dump-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar"

dump="/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/zu/zu.preprocessed.xml"
language="zu"
output="/home/gbecan/git/OpenCompare/wikipedia-dump-analysis/output/"

spark-submit --class $class --master $master $jar $dump $language $output $@

