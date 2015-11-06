package org.opencompare.analysis

import java.io.{BufferedInputStream, FileInputStream, File}

import akka.actor.{ActorRef, Terminated, Props, Actor}
import akka.actor.Actor.Receive
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.spark.streaming.receiver.ActorHelper

import scala.io.Source
import scala.xml.pull._

/**
 * Created by gbecan on 11/5/15.
 */
class WikipediaDumpReader(dumpFile : File) extends Actor {

  var receiver : ActorRef = _

  // Create compressed input stream
  val fin = new FileInputStream(dumpFile)
  val in = new BufferedInputStream(fin)
  val bzIn = new BZip2CompressorInputStream(in, true)

  // Create XML reader
  val sourceFile = Source.fromInputStream(bzIn)
  val xmlReader = new XMLEventReader(sourceFile)

  var nbOfTables = 0
  var nbOfRevs = 0
  var nbOfRevsWithTables = 0

  var inRevision = false
  var inRevisionContent = false
  var inRevId = false
  var doneRevId = false

  var revContainsTable = false

  var revId = ""



  override def receive: Receive = {
    case RegisterReceiver(actor) =>
      receiver = actor
    case NextPage() =>
      println("next page")

      for (event <- xmlReader) {
        event match {
          case EvElemStart(pre, label, attrs, scope) =>
            label match {
              case "revision" =>
                inRevision = true
                doneRevId = false
              case "text" if inRevision => inRevisionContent = true
              case "id" if inRevision => inRevId = true
              case _ =>
            }
          case EvElemEnd(pre, label) =>
            label match {
              case "revision" =>
                nbOfRevs += 1
                inRevision = false
                if (revContainsTable) {
                  nbOfRevsWithTables += 1

                  if (nbOfRevsWithTables % 1000 == 0) {
                    println(nbOfRevsWithTables + " / " + nbOfRevs)
                  }

                }
                revContainsTable =false
              case "text" if inRevisionContent => inRevisionContent = false
              case "id" if inRevId => doneRevId = true
              case _ =>
            }

          case EvEntityRef(entity) =>
          case EvProcInstr(target, text) =>
          case EvText(text) if inRevisionContent =>
            if (text.contains("{|")) {
              revContainsTable = true
              //            println(revId + " : table detected")
              //            if (nbOfTables % 1000 == 0) {
              //              println(nbOfTables)
              //            }
              nbOfTables += 1
              receiver ! nbOfTables
            }
          case EvText(text) if inRevId && !doneRevId => revId = text
          case EvText(text) =>
          case EvComment(text) =>
        }
      }

    case Terminated(self) =>
      bzIn.close()

      println("#revs = " + nbOfRevs)
      println("#revs with tables = " + nbOfRevsWithTables)
      println("#tables = " + nbOfTables)
  }

}

object WikipediaDumpReader {
  def props(dumpFile : File) = Props(new WikipediaDumpReader(dumpFile))
}

trait DumpReaderMessage
case class NextPage() extends DumpReaderMessage
case class RegisterReceiver(receiver : ActorRef) extends DumpReaderMessage