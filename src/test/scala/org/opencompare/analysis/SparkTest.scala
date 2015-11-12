package org.opencompare.analysis

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

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

  val preprocessedDumpFile = zuPreprocessedXMLFile
  val language = "zu"
  val outputDirectory = new File("output/")
  outputDirectory.mkdirs() // Prepare output directory
  val minPartitions = Some(50)




  val sparkConf = new SparkConf()
    .setAppName("wikipedia-analysis")
    .setMaster("local[4]")

    val sparkContext = new SparkContext(sparkConf)

  ignore should "preprocess dump file" in {
    val dumpPreprocessor = new WikipediaDumpPreprocessor
    dumpPreprocessor.preprocessDump(zuDumpFile, preprocessedDumpFile)
  }

  ignore should "parse dump file with spark" in {

    val dumpProcessor = new WikipediaDumpProcessor
    val stats = dumpProcessor.process(sparkContext, preprocessedDumpFile, language, outputDirectory, minPartitions)

    println("Pages without PCM = " + stats.filter(_.isEmpty).size)
    println("Pages with PCMs = " + stats.filter(_.nonEmpty).size)
    val sizes = stats.filter(_.nonEmpty).map(_.size)
    println("Min number of PCMs = " + sizes.min)
    println("Avg number of PCMs = " + (sizes.sum.toDouble / sizes.size.toDouble))
    println("Max number of PCMs = " + sizes.max)

  }

}
