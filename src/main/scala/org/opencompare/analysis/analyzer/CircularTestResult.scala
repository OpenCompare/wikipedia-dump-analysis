package org.opencompare.analysis.analyzer

/**
  * Created by gbecan on 11/12/15.
  */
case class CircularTestResult(name : String, samePCM : Boolean, sameMetadata : Boolean) {
  def and(other : CircularTestResult) : CircularTestResult = {
    CircularTestResult("and", samePCM && other.samePCM, sameMetadata && other.sameMetadata)
  }
}
