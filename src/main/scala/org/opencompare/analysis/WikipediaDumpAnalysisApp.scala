package org.opencompare.analysis

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkContext, SparkConf}
import org.opencompare.api.java.impl.io.KMFJSONExporter
import org.opencompare.io.wikipedia.io.{WikiTextLoader, WikiTextTemplateProcessor, MediaWikiAPI}

import scala.util.Random

import collection.JavaConversions._

/**
  * Parameters
  * - path to dump file
  * - language
  * - output directory
  * - minimum number of partitions
  */
object WikipediaDumpAnalysisApp {


  def main(args: Array[String]) {
    if (args.size < 3) {

      println("USAGE : path_to_dump_file language output_directory [min_partitions]")

    } else {

      val dumpFile = new File(args(0))
      val language = args(1)
      val outputDirectory = new File(args(2))
      outputDirectory.mkdirs()
      val minPartitions = args.lift(3).map(_.toInt)

      println("dump file = " + dumpFile.getAbsolutePath)
      println("language = " + language)
      println("output dir = " + outputDirectory.getAbsolutePath)
      println("min partitions = " + minPartitions.getOrElse("default"))

      val sparkConf = new SparkConf()
        .setAppName("Wikipedia dump analysis")

      val sparkContext = new SparkContext(sparkConf)

      val dumpProcessor = new WikipediaDumpProcessor
      val stats = dumpProcessor.process(sparkContext, dumpFile, language, outputDirectory, minPartitions)

      //    stats.foreach(println)

      println("Pages without PCM = " + stats.filter(_.isEmpty).size)
      println("Pages with PCMs = " + stats.filter(_.nonEmpty).size)
      val sizes = stats.filter(_.nonEmpty).map(_.size)
      println("Min number of PCMs = " + sizes.min)
      println("Avg number of PCMs = " + (sizes.sum.toDouble / sizes.size.toDouble))
      println("Max number of PCMs = " + sizes.max)


    }
  }

}
