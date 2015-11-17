package org.opencompare.analysis

import java.io.{PrintWriter, StringWriter, File}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.github.tototoshi.csv.CSVWriter
import org.apache.log4j.Logger
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

      println("USAGE : path_to_dump_file language output_directory export_pcm [min_partitions]")

    } else {

      // Read parameters
      val dumpFile = new File(args(0))
      val language = args(1)
      val outputDirectory = new File(args(2))
      val exportPCM = args(3).toBoolean
      val minPartitions = args.lift(4).map(_.toInt)

      println("dump file = " + dumpFile.getAbsolutePath)
      println("language = " + language)
      println("output dir = " + outputDirectory.getAbsolutePath)
      println("export pcm = " + exportPCM)
      println("min partitions = " + minPartitions.getOrElse("default"))

      // Create output directory structure
      outputDirectory.mkdirs()
      new File(outputDirectory.getAbsolutePath + "/pcms").mkdirs()

      // Create Spark context
      val sparkConf = new SparkConf()
        .setAppName("Wikipedia dump analysis")

      val sparkContext = new SparkContext(sparkConf)
      sparkContext.setLogLevel("INFO")

      // Process dump
      val dumpProcessor = new WikipediaDumpProcessor
      val results = dumpProcessor.process(sparkContext, dumpFile, language, outputDirectory, exportPCM, minPartitions)

      // Writer results to CSV
      val writer = CSVWriter.open(outputDirectory.getAbsolutePath + "/stats.csv")

      writer.writeRow(Seq("id", "title", "status", "filename", "features", "products"))

      for (result <- results) {
        for (stats <- result) {
            stats match {
              case PCMStats(id, title, filename, features, products) =>
                writer.writeRow(Seq(id, title, "ok", filename, features, products))
              case Error(id, title, stackTrace) =>
                writer.writeRow(Seq(id, title, stackTrace))
            }
        }
      }

      writer.close()

      // Print some stats
      println("Pages without PCM = " + results.filter(_.isEmpty).size)
      println("Pages with PCMs = " + results.filter(_.nonEmpty).size)
      val sizes = results.filter(_.nonEmpty).map(_.size)
      println("Min number of PCMs = " + sizes.min)
      println("Avg number of PCMs = " + (sizes.sum.toDouble / sizes.size.toDouble))
      println("Max number of PCMs = " + sizes.max)


    }
  }

}