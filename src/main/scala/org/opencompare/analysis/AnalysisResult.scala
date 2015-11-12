package org.opencompare.analysis

/**
  * Created by gbecan on 12/11/15.
  */
trait AnalysisResult
case class PCMStats(title : String, features : Int, products : Int) extends AnalysisResult
case class Error(exception : Throwable) extends AnalysisResult
