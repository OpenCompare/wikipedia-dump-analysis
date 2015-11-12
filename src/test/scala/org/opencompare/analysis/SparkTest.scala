package org.opencompare.analysis

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Path, Files}

import akka.actor.ActorSystem
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.opencompare.api.java.impl.io.KMFJSONExporter
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextTemplateProcessor, WikiTextLoader}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random
import scala.xml.XML
import collection.JavaConversions._

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
  val minPartitions = 50

  new File("output/").mkdirs() // Prepare output directory

  val sparkConf = new SparkConf()
    .setAppName("wikipedia-analysis")
    .setMaster("local[4]")

    val sparkContext = new SparkContext(sparkConf)
//  val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(1))
//  val sqlContext = new SQLContext(sparkContext)

  ignore should "preprocess dump file" in {
    val dumpPreprocessor = new WikipediaDumpPreprocessor
    dumpPreprocessor.preprocessDump(zuDumpFile, preprocessedDumpFile)
  }

  it should "parse dump file with spark" in {

    val xmlPages = sparkContext.textFile(preprocessedDumpFile.getAbsolutePath, minPartitions)

    val pages = xmlPages.map { doc =>
      // Parse XML
      val pageParser = new PageParser
      val page = pageParser.parseDump(doc)

      // Mine PCM
      val mediaWikiAPI = new MediaWikiAPI("wikipedia.org")
      val templateProcessor = new WikiTextTemplateProcessor(mediaWikiAPI)
      val wikitextMiner = new WikiTextLoader(templateProcessor)

      val result : List[AnalysisResult] = try {
        val pcmContainers = wikitextMiner.mine("zu", page.revision.wikitext, page.title)

        val stats = for (pcmContainer <- pcmContainers) yield {
          // Write PCM to disk
          val exporter = new KMFJSONExporter
          val json = exporter.export(pcmContainer)
          val sanitizedName = pcmContainer.getPcm.getName.replaceAll("[^a-zA-Z0-9.\\-_]", "_")
          val fileName = if (sanitizedName.isEmpty) {
            Random.nextString(10)
          } else {
            sanitizedName
          }
          val outputPath = Paths.get("output", fileName)
          Files.write(outputPath, List(json), StandardCharsets.UTF_8)


          // Compute stats
          val title = pcmContainer.getPcm.getName
          val nbFeatures = pcmContainer.getPcm.getConcreteFeatures.size()
          val nbProducts = pcmContainer.getPcm.getProducts.size()

          PCMStats(title, nbFeatures, nbProducts)
        }

        stats.toList
      } catch {
        case e : Throwable => List(Error(e))
      }

      result
    }

    val stats = pages.collect()
//    stats.foreach(println)

    println("Pages without PCM = " + stats.filter(_.isEmpty).size)
    println("Pages with PCMs = " + stats.filter(_.nonEmpty).size)
    val sizes = stats.filter(_.nonEmpty).map(_.size)
    println("Min number of PCMs = " + sizes.min)
    println("Avg number of PCMs = " + (sizes.sum.toDouble / sizes.size.toDouble))
    println("Max number of PCMs = " + sizes.max)

  }

}
