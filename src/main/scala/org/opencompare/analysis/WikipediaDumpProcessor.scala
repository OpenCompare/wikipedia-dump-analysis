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
  * Created by gbecan on 12/11/15.
  */
class WikipediaDumpProcessor {

  def process(sparkContext : SparkContext, dumpFile : File, language : String, outputDirectory : File, minPartitions : Option[Int]) : Array[List[AnalysisResult]] = {

    val pages = if (minPartitions.isDefined) {
      sparkContext.textFile(dumpFile.getAbsolutePath, minPartitions.get)
    } else {
      sparkContext.textFile(dumpFile.getAbsolutePath)
    }

    val stats = pages.map { doc =>
      // Parse XML
      val pageParser = new PageParser
      val page = pageParser.parseDump(doc)

      // Mine PCM
      val mediaWikiAPI = new MediaWikiAPI("wikipedia.org")
      val templateProcessor = new WikiTextTemplateProcessor(mediaWikiAPI) {
        override def expandTemplate(language: String, template: String): String = template
      }
      val wikitextMiner = new WikiTextLoader(templateProcessor)

      val result : List[AnalysisResult] = try {
        val pcmContainers = wikitextMiner.mine(language, page.revision.wikitext, page.title)

        val stats = for ((pcmContainer, index) <- pcmContainers.zipWithIndex) yield {
          // Write PCM to disk
          val exporter = new KMFJSONExporter
          val json = exporter.export(pcmContainer)
          val fileName = page.title.replaceAll("[^a-zA-Z0-9.\\-_]", "_") + "_" + index + ".pcm"

          val outputPath = Paths.get(outputDirectory.getAbsolutePath, "pcms", fileName)
          Files.write(outputPath, List(json), StandardCharsets.UTF_8)


          // Compute stats
          val nbFeatures = pcmContainer.getPcm.getConcreteFeatures.size()
          val nbProducts = pcmContainer.getPcm.getProducts.size()

          PCMStats(page.id, page.title, nbFeatures, nbProducts)
        }

        stats.toList
      } catch {
        case e : Throwable =>
          List(Error(page.id, page.title, e))
      }

      result
    }

    stats.collect()
  }

}
