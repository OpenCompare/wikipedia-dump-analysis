package org.opencompare.analysis.analyzer

import org.opencompare.api.java.{PCMFactory, PCMContainer}
import org.opencompare.api.java.impl.PCMFactoryImpl
import org.opencompare.api.java.impl.io.{KMFJSONLoader, KMFJSONExporter}
import org.opencompare.api.java.io._
import org.opencompare.io.wikipedia.io.{WikiTextTemplateProcessor, WikiTextExporter, WikiTextLoader}

import collection.JavaConversions._

/**
  * Created by gbecan on 11/12/15.
  */
class CircularTestAnalyzer(val factory: PCMFactory) {

  def kmfJson(pcmContainer : PCMContainer) : CircularTestResult = {
    circularTest(pcmContainer, "kmfJson", new KMFJSONLoader(), new KMFJSONExporter())
  }

  def csv(pcmContainer : PCMContainer) : CircularTestResult = {
    circularTest(pcmContainer, "csv", new CSVLoader(factory), new CSVExporter)
  }

  def html(pcmContainer : PCMContainer) : CircularTestResult = {
    circularTest(pcmContainer, "html", new HTMLLoader(factory), new HTMLExporter())
  }

  def wikitext(pcmContainer : PCMContainer, templateProcessor : WikiTextTemplateProcessor) : CircularTestResult = {
    circularTest(pcmContainer, "wikitext", new WikiTextLoader(templateProcessor), new WikiTextExporter())
  }

  def circularTest(pcmContainer : PCMContainer, name : String, loader : PCMLoader, exporter : PCMExporter) : CircularTestResult = {
    try {
      val pcmContainer2 = loader.load(exporter.export(pcmContainer)).head

      val samePCM = pcmContainer.getPcm == pcmContainer2.getPcm
      val sameMetadata = pcmContainer.getMetadata == pcmContainer2.getMetadata
      CircularTestResult(name, samePCM, sameMetadata)
    } catch {
      case e : Exception => CircularTestResult(name, false, false)
    }

  }

}

