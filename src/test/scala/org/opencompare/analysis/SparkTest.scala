package org.opencompare.analysis

import java.io.File

import akka.actor.ActorSystem
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextTemplateProcessor, WikiTextLoader}
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.XML
import collection.JavaConversions._

/**
 * Created by gbecan on 11/5/15.
 */
class SparkTest extends FlatSpec with Matchers {

  val articleNamesDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/enwiki-20151102-pages-articles-multistream-index.txt.bz2")
  val enDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/enwiki-20151102-pages-articles-multistream.xml.bz2")
  val zuDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/zu/zuwiki-20150806-pages-articles-multistream.xml.bz2")

  val preprocessedDumpFile = new File("zu-output.xml.bz2")

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

    val xmlPages = sparkContext.textFile(preprocessedDumpFile.getAbsolutePath)

    val pages = xmlPages.map { doc =>
        val pageXML = XML.loadString(doc)
        val revXML = (pageXML \ "revision").head

        val title = (pageXML \ "title").head.text
        val id = (pageXML \ "id").head.text

        val revId = (revXML \ "id").head.text
        val revParentIdOption = (revXML \ "parentid").headOption
        val revParentId = if (revParentIdOption.isDefined) {
          revParentIdOption.get.text
        } else {
          ""
        }
        val timestamp = (revXML \ "timestamp").head.text
        val wikitext = (revXML \ "text").head.text

        val revision = Revision(revId, revParentId, timestamp, wikitext)
        val page = Page(id, title, revision)
        page
      }

    val miningResults = pages.map { page =>
      try {
        val mediaWikiAPI = new MediaWikiAPI("wikipedia.org")
        val templateProcessor = new WikiTextTemplateProcessor(mediaWikiAPI)
        val wikitextMiner = new WikiTextLoader(templateProcessor)
        val pcmContainers = wikitextMiner.mine("zu", page.revision.wikitext, page.title)
        Some(pcmContainers)
      } catch {
        case e : Throwable => None
      }
    }.cache()

    val pcmContainers = miningResults.filter(_.isDefined).flatMap(_.get)

    val stats = pcmContainers.map { pcmContainer =>
      val title = pcmContainer.getPcm.getName
      val nbFeatures = pcmContainer.getPcm.getConcreteFeatures.size()
      val nbProducts = pcmContainer.getPcm.getProducts.size()
      (title, nbFeatures, nbProducts)
    }

    val errors = miningResults.filter(!_.isDefined).count()

    stats.collect().foreach(println)
    println(errors)

  }

}
