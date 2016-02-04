package org.opencompare.analysis

import org.opencompare.analysis.analyzer.CircularTestResult

/**
  * Created by gbecan on 12/11/15.
  */
trait AnalysisResult
case class PCMStats(id : String, title : String, filename : String, circularTest : List[CircularTestResult], features : Int, products : Int, featureDepth : Int, emptyCells : Int) extends AnalysisResult
case class Error(id : String, title : String, stackTrace : String) extends AnalysisResult
