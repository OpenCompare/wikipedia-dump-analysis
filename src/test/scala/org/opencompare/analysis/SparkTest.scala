package org.opencompare.analysis

import java.io.File

import com.github.tototoshi.csv.CSVWriter
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.XML

/**
 * Created by gbecan on 11/5/15.
 */
class SparkTest extends FlatSpec with Matchers {

  val articleNamesDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/enwiki-20151102-pages-articles-multistream-index.txt.bz2")
  val enDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/enwiki-20151102-pages-articles-multistream.xml.bz2")
  val enPreprocessedDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/en.preprocessed.xml.bz2")

  val zuDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/zu/zuwiki-20150806-pages-articles-multistream.xml.bz2")
  val zuPreprocessedDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/zu/zu.preprocessed.xml.bz2")
  val zuPreprocessedXMLFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/zu/zu.preprocessed.xml")

  val preprocessedDumpFile = zuPreprocessedDumpFile
  val language = "zu"
  val outputDirectory = new File("output/")
  outputDirectory.mkdirs() // Prepare output directory
  val minPartitions = Some(10000)




  val sparkConf = new SparkConf()
    .setAppName("wikipedia-analysis")
    .setMaster("local[1]")

    val sparkContext = new SparkContext(sparkConf)

  ignore should "preprocess dump file" in {
    val dumpPreprocessor = new WikipediaDumpPreprocessor
    dumpPreprocessor.preprocessDump(zuDumpFile, preprocessedDumpFile)
  }

  it should "process dump file" in {

    val dumpProcessor = new WikipediaDumpProcessor
    val results = dumpProcessor.process(sparkContext, preprocessedDumpFile, language, outputDirectory, exportPCM = true, minPartitions)

    WikipediaDumpAnalysisApp.writeResultsToCSV(outputDirectory, results)

    println("Pages without PCM = " + results.filter(_.isEmpty).size)
    println("Pages with PCMs = " + results.filter(_.nonEmpty).size)
    val sizes = results.filter(_.nonEmpty).map(_.size)
    println("Min number of PCMs = " + sizes.min)
    println("Avg number of PCMs = " + (sizes.sum.toDouble / sizes.size.toDouble))
    println("Max number of PCMs = " + sizes.max)

  }

  ignore should "count content pages" in {
    val pages = sparkContext.textFile(preprocessedDumpFile.getAbsolutePath, minPartitions.get)
    val types = pages.map { page =>

      val xmlPages = XML.loadString(page)

      val title = (xmlPages \ "title").head.text

      val redirect = (xmlPages \ "redirect").nonEmpty
      val specialPages = Set("Help", "File", "Wikipedia", "WP", "Category", "Portal", "MediaWiki", "Template", "Module", "Special")
      val special =  specialPages.exists(e => title.startsWith(e + ":"))

      val namespace = (xmlPages \ "ns").head.text.toInt

      (title, redirect, special, namespace)
    }.cache()

    println("all pages = " + types.count())
    println("content pages = " + types.filter(t => !t._2 && !t._3).count())
    println("content pages (ns) = " + types.filter(t => t._4 == 0 && !t._2).count())
    println("redirects = " + types.filter(t => t._2).count())
    println("specials = " + types.filter(t => t._3).count())
    println("namespaces = " + types.map(_._4).distinct().collect().mkString("(", ", ", ")"))

    types.filter(t => t._3).map(t => t._1.substring(0, t._1.indexOf(":"))).distinct()//.foreach(println)


    val contentPages = types.filter(t => !t._2 && !t._3).map(_._1).collect()
    val contentPagesNs = types.filter(t => t._4 == 0 && !t._2).map(_._1).collect()

    contentPagesNs.toSet.diff(contentPages.toSet).foreach(println)
  }

  ignore should "list namespaces" in {
    val pages = sparkContext.textFile(preprocessedDumpFile.getAbsolutePath, minPartitions.get)
    val namespaces = pages.map { page =>

      val xmlPages = XML.loadString(page)
      val namespace = (xmlPages \ "ns").head.text.toInt

      namespace
    }

    println(namespaces.countByValue())
  }

}
