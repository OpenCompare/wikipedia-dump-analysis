package org.opencompare.analysis

import org.opencompare.analysis.analyzer.{ValueResult, CircularTestResult}

/**
  * Created by gbecan on 12/11/15.
  */
trait AnalysisResult
case class PCMStats(
                     id : String,
                     title : String,
                     filename : String,
                     circularTest : List[CircularTestResult],
                     rows : Int,
                     columns : Int,
                     features : Int,
                     products : Int,
                     featureDepth : Int,
                     cells : Int,
                     emptyCells : Int,
                     valueResult : ValueResult,
                     templates : Int) extends AnalysisResult
case class Error(id : String, title : String, stackTrace : String) extends AnalysisResult
