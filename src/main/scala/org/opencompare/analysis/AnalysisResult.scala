package org.opencompare.analysis

/**
  * Created by gbecan on 12/11/15.
  */
trait AnalysisResult
case class PCMStats(id : String, title : String, filename : String, features : Int, products : Int) extends AnalysisResult
case class Error(id : String, title : String, stackTrace : String) extends AnalysisResult
