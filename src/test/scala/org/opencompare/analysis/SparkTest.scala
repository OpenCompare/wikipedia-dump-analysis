package org.opencompare.analysis

import java.io.File

import akka.actor.ActorSystem
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.XML

/**
 * Created by gbecan on 11/5/15.
 */
class SparkTest extends FlatSpec with Matchers {

  val articleNamesDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/enwiki-20151102-pages-articles-multistream-index.txt.bz2")
  val enDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/enwiki-20151102-pages-articles-multistream.xml.bz2")
  val zuDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/zu/zuwiki-20150806-pages-articles-multistream.xml.bz2")

  val preprocessedDumpFile = new File("output.xml.bz2")

  val sparkConf = new SparkConf()
    .setAppName("wikipedia-analysis")
    .setMaster("local[4]")

    val sparkContext = new SparkContext(sparkConf)
//  val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(1))
//  val sqlContext = new SQLContext(sparkContext)

  it should "do stuff" in {

    val dumpPreprocessor = new WikipediaDumpPreprocessor
    dumpPreprocessor.preprocessDump(zuDumpFile, preprocessedDumpFile)

    val xmlDocs = sparkContext
      .textFile(preprocessedDumpFile.getAbsolutePath)
      .map(doc => XML.loadString(doc))
      .first()

    println(xmlDocs)

  }

}
