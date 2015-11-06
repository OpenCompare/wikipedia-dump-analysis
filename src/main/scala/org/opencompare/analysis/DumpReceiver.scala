package org.opencompare.analysis

import akka.actor.{ActorPath, ActorRef, Props, Actor}
import akka.actor.Actor.Receive
import org.apache.spark.streaming.receiver.ActorHelper

/**
  * Created by gbecan on 06/11/15.
  */
class DumpReceiver(dumpReaderPath : ActorPath) extends Actor with ActorHelper {
  context.actorSelection(dumpReaderPath) ! RegisterReceiver(self)

  override def receive: Receive = {
    case value : Int => store(value)
  }
}

object DumpReceiver {
  def props(dumpReader : ActorPath) = Props(new DumpReceiver(dumpReader))
}