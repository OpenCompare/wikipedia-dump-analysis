package org.opencompare.analysis

import java.io.File

import akka.actor.ActorSystem
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by gbecan on 11/5/15.
 */
class SparkTest extends FlatSpec with Matchers {

  val articleNamesDumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/enwiki-20151102-pages-articles-multistream-index.txt.bz2")
  val dumpFile = new File("/home/gbecan/Documents/dev/opencompare/wikipedia-dumps/en/enwiki-20151102-pages-articles-multistream.xml.bz2")


  val actorSystem = ActorSystem("actorSystem")

  val sparkConf = new SparkConf()
    .setAppName("wikipedia-analysis")
    .setMaster("local[4]")
//  val sparkContext = new SparkContext(sparkConf)
//  val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(1))
//  val sqlContext = new SQLContext(sparkContext)

  it should "do stuff" in {

    val searchedString = "Comparison of "

    val dumpReaderActor = actorSystem.actorOf(WikipediaDumpReader.props(dumpFile), "DumpReader")
    println(dumpReaderActor.path.toSerializationFormat)
//    val nbOfTables = sparkStreamingContext.actorStream[Int](DumpReceiver.props(dumpReaderActor.path), "DumpReceiver")
//
//    dumpReaderActor ! NextPage()
//
//    nbOfTables.foreachRDD(rdd => rdd.collect().foreach(println))
//
//    sparkStreamingContext.start()
//    sparkStreamingContext.awaitTermination()

  }

}
