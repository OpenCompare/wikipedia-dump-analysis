package org.opencompare.analysis

import java.io.{PrintWriter, StringWriter, File}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkContext, SparkConf}
import org.opencompare.analysis.analyzer.CircularTestAnalyzer
import org.opencompare.api.java.impl.PCMFactoryImpl
import org.opencompare.api.java.impl.io.KMFJSONExporter
import org.opencompare.io.wikipedia.io.{WikiTextLoader, WikiTextTemplateProcessor, MediaWikiAPI}

import scala.util.Random

import collection.JavaConversions._

/**
  * Created by gbecan on 12/11/15.
  */
class WikipediaDumpProcessor {

  def process(sparkContext : SparkContext, dumpFile : File, language : String, outputDirectory : File, exportPCM : Boolean, minPartitions : Option[Int]) : Array[List[AnalysisResult]] = {

    val pages = if (minPartitions.isDefined) {
      sparkContext.textFile(dumpFile.getAbsolutePath, minPartitions.get)
    } else {
      sparkContext.textFile(dumpFile.getAbsolutePath)
    }

    val stats = pages
      .map { doc =>
        // Parse XML
        val pageParser = new PageParser
        val page = pageParser.parseDump(doc)
        page
      }
      .filter { page =>
        page.namespace == 0 && !page.redirect && page.revision.wikitext.contains("[[")
      }
      .map { page =>
        // Mine PCM
        val factory = new PCMFactoryImpl
        val mediaWikiAPI = new MediaWikiAPI("wikipedia.org")
        val templateProcessor = new WikiTextTemplateProcessor(mediaWikiAPI) {
          override def expandTemplate(language: String, template: String): String = {
            template
              .replaceAll("\\{", "")
              .replaceAll("\\}", "")
              .replaceAll("\\|", "")
          }
        }
        val wikitextMiner = new WikiTextLoader(templateProcessor)

        val result : List[AnalysisResult] = try {
          val pcmContainers = wikitextMiner.mine(language, page.revision.wikitext, page.title)

          val stats = for ((pcmContainer, index) <- pcmContainers.zipWithIndex) yield {

            // Circular test
            val circularTestAnalyzer = new CircularTestAnalyzer(factory)
            val kmfCircular = circularTestAnalyzer.kmfJson(pcmContainer)
            val csvCircular = circularTestAnalyzer.csv(pcmContainer)
            val htmlCircular = circularTestAnalyzer.html(pcmContainer)
            val wikitextCircular = circularTestAnalyzer.wikitext(pcmContainer, templateProcessor)

            // Write PCM to disk
            val fileName = page.title.replaceAll("[^a-zA-Z0-9.\\-_]", "_") + "_" + index + ".pcm"

            if (exportPCM) {
              val exporter = new KMFJSONExporter
              val json = exporter.export(pcmContainer)
              val outputPath = Paths.get(outputDirectory.getAbsolutePath, "pcms", fileName)
              Files.write(outputPath, List(json), StandardCharsets.UTF_8)
            }

            // Compute stats
            val pcm = pcmContainer.getPcm

            val nbFeatures = pcm.getConcreteFeatures.size()
            val nbProducts = pcm.getProducts.size()
            val featureDepth = pcm.getFeaturesDepth
            val emptyCells = pcm.getProducts.flatMap(_.getCells).count(_.getContent.isEmpty)

            PCMStats(
              page.id,
              page.title,
              fileName,
              List(kmfCircular, csvCircular, htmlCircular, wikitextCircular),
              nbFeatures,
              nbProducts,
              featureDepth,
              emptyCells)
          }

          stats.toList
        } catch {
          case e : Throwable =>
            val stackTraceSWriter = new StringWriter()
            val stackTracePWriter = new PrintWriter(stackTraceSWriter)
            e.printStackTrace(stackTracePWriter)
            val stackTrace = stackTraceSWriter.toString
            List(Error(page.id, page.title, stackTrace))
        }

        result
      }

    stats.collect()
  }

}
