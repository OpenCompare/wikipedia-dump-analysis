package org.opencompare.analysis

import java.io._

import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorOutputStream, BZip2CompressorInputStream}

import scala.io.Source
import scala.xml.pull._

/**
  * Created by gbecan on 06/11/15.
  */
class WikipediaDumpPreprocessor {

  def preprocessDump(dumpFile : File, outputFile : File) {
    // Create compressed input stream
    val fin = new FileInputStream(dumpFile)
    val in = new BufferedInputStream(fin)
    val bzIn = new BZip2CompressorInputStream(in, true)

    // Create XML reader
    val sourceFile = Source.fromInputStream(bzIn)
    val xmlReader = new XMLEventReader(sourceFile)

    // Create compressed output stream
    val fout = new FileOutputStream(outputFile)
    val out = new BufferedOutputStream(fout)
    val bzOut = new BZip2CompressorOutputStream(out)
    val writer = new PrintWriter(bzOut)


    val pageBuilder = new StringBuilder
    var inPage = false

    for (event <- xmlReader) {
      event match {
        case EvElemStart(pre, "page", attrs, scope) =>
          inPage = true
          pageBuilder ++= "<page>"

        case EvElemStart(pre, label, attrs, scope) if inPage =>
          pageBuilder ++= "<" + label + ">"

        case EvElemStart(pre, label, attrs, scope) =>

        case EvElemEnd(pre, "page") =>
          inPage = false
          pageBuilder ++= "</page>"
          val revisionText = pageBuilder.toString()
          writer.print(revisionText.replaceAll("\n","&#10;"))
          writer.print("\n")
          pageBuilder.clear()

        case EvElemEnd(pre, label) if inPage =>
          pageBuilder ++= "</" + label + ">"

        case EvElemEnd(pre, label) =>

        case EvEntityRef(entity) if inPage =>
          pageBuilder ++= "&" + entity + ";"
        case EvEntityRef(entity) =>

        case EvProcInstr(target, text) =>

        case EvText(text) if inPage =>
          pageBuilder ++= text

        case EvText(text) =>

        case EvComment(text) =>
      }
    }

    bzIn.close()
    writer.close()
  }
}
