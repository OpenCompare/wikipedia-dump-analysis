package org.opencompare.analysis

import java.io.{File, PrintWriter, StringWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.github.tototoshi.csv.CSVWriter
import org.apache.spark.{SparkConf, SparkContext}
import org.opencompare.analysis.analyzer.{CircularTestAnalyzer, TemplateAnalyzer, ValueAnalyzer}
import org.opencompare.api.java.extractor.CellContentInterpreter
import org.opencompare.api.java.impl.PCMFactoryImpl
import org.opencompare.api.java.impl.io.KMFJSONExporter
import org.opencompare.api.java.io.{ImportMatrixLoader, PCMDirection}
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextLoader, WikiTextTemplateProcessor}

import scala.util.Random
import collection.JavaConversions._

/**
  * Created by gbecan on 12/11/15.
  */
class WikipediaDumpProcessor {

  def process(sparkContext : SparkContext, dumpFile : File, language : String, outputDirectory : File, exportPCM : Boolean, minPartitions : Option[Int]) : Array[List[AnalysisResult]] = {

    // Open dump file
    val docs = if (minPartitions.isDefined) {
      sparkContext.textFile(dumpFile.getAbsolutePath, minPartitions.get)
    } else {
      sparkContext.textFile(dumpFile.getAbsolutePath)
    }

    // Parse pages
    val pages = docs
      .map { doc =>
        // Parse XML
        val pageParser = new PageParser
        val page = pageParser.parseDump(doc)
        page
      }
      .filter { page =>
        page.namespace == 0 && !page.redirect && page.revision.wikitext.contains("[[")
      }

    // Analyze PCMs
    val stats = pages.map { page =>
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
        val ioLoader = new ImportMatrixLoader(factory, new CellContentInterpreter(factory), PCMDirection.UNKNOWN)
        val wikitextMiner = new WikiTextLoader(templateProcessor)



        val result : List[AnalysisResult] = try {

          val importMatrices = wikitextMiner.mineImportMatrix(language, page.revision.wikitext, page.title)
          val pcmContainers = importMatrices.map(ioLoader.load)

          val stats = for (((pcmContainer, importMatrix), index) <- pcmContainers.zip(importMatrices).zipWithIndex) yield {

            // Circular test
            val circularTestAnalyzer = new CircularTestAnalyzer(factory)
            val kmfCircular = circularTestAnalyzer.kmfJson(pcmContainer)
            val csvCircular = circularTestAnalyzer.csv(pcmContainer)
            val htmlCircular = circularTestAnalyzer.html(pcmContainer)
            val wikitextCircular = circularTestAnalyzer.wikitext(pcmContainer, templateProcessor)
            val allCircular = kmfCircular and csvCircular and htmlCircular and wikitextCircular

            // Write PCM to disk
            val baseName = page.title.replaceAll("[^a-zA-Z0-9.\\-_]", "_") + "_" + index
            val fileName = baseName + ".pcm"

            if (exportPCM) {
              val exporter = new KMFJSONExporter
              val json = exporter.export(pcmContainer)
              val outputPath = Paths.get(outputDirectory.getAbsolutePath, "pcms", fileName)
              Files.write(outputPath, List(json), StandardCharsets.UTF_8)
            }

            // Write debug info
//            if (!allCircular.samePCM) {
//              val outputPath = Paths.get(outputDirectory.getAbsolutePath, "reports", baseName + ".wikitext")
//              val debugInfo = page.revision.wikitext
//              Files.write(outputPath, List(debugInfo), StandardCharsets.UTF_8)
//            }

            // Compute stats
            val pcm = pcmContainer.getPcm

            val isValid = pcm.isValid

            val nbRows = importMatrix.getNumberOfRows
            val nbColumns = importMatrix.getNumberOfColumns

            val nbFeatures = pcm.getConcreteFeatures.size()
            val nbProducts = pcm.getProducts.size()
            val featureDepth = pcm.getFeaturesDepth
            val nbCells = pcm.getProducts.flatMap(_.getCells).size
            val emptyCells = pcm.getProducts.flatMap(_.getCells).count(_.getContent.isEmpty)

            val valueAnalyzer = new ValueAnalyzer
            val valueResult = valueAnalyzer.analyze(pcm)

            val templateAnalyzer = new TemplateAnalyzer
            val templateResult = templateAnalyzer.analyzeTemplates(importMatrix, page.title)
            val templates = templateResult.values.sum

            // Write templates
//            if (templateResult.nonEmpty) {
//              val writer = CSVWriter.open(outputDirectory.getAbsolutePath + "/templates/" + page.id + ".csv")
//              writer.writeRow(Seq("name", "count"))
//              templateResult.foreach { result =>
//                writer.writeRow(Seq(result._1, result._2))
//              }
//              writer.close()
//            }


            PCMStats(
              page.id,
              page.title,
              fileName,
              List(kmfCircular, csvCircular, htmlCircular, wikitextCircular),
              isValid,
              nbRows,
              nbColumns,
              nbFeatures,
              nbProducts,
              featureDepth,
              nbCells,
              emptyCells,
              valueResult,
              templates)
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
      }.collect()


    // Analyze templates
    val templateStats = pages.map { page =>
      try {

        // Init
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
        val importMatrices = wikitextMiner.mineImportMatrix(language, page.revision.wikitext, page.title)

        val templateAnalyze = new TemplateAnalyzer

        // Count usage of templates
        importMatrices.map { importMatrix =>
          templateAnalyze.analyzeTemplates(importMatrix, page.title)
        }.fold(Map.empty[String, Int]) { (a, b) =>
          val merged = (a /: b) { case (map, (k,v)) =>
            map + ( k -> (v + map.getOrElse(k, 0)) )
          }
          merged
        }
      } catch {
        case _ : Throwable => Map.empty[String, Int]
      }
    }.fold(Map.empty[String, Int]) { (a, b) =>
      val merged = (a /: b) { case (map, (k,v)) =>
        map + ( k -> (v + map.getOrElse(k, 0)) )
      }
      merged
    }

    val writer = CSVWriter.open(outputDirectory.getAbsolutePath + "/stats-templates.csv")
    writer.writeRow(Seq("name", "count"))

    templateStats.foreach { result =>
      writer.writeRow(Seq(result._1, result._2))
      writer.flush()
    }

    writer.close()


    // Return statistics
    stats
  }


}
