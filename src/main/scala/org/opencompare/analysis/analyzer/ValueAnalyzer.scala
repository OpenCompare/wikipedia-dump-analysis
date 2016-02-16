package org.opencompare.analysis.analyzer

import org.opencompare.api.java.PCM
import org.opencompare.api.java.value._

import collection.JavaConversions._

/**
  * Created by gbecan on 16/02/16.
  */
class ValueAnalyzer {

  def analyze(pcm : PCM) : ValueResult = {

    var countNoInterpretation = 0
    var countBoolean = 0
    var countConditional = 0
    var countDate = 0
    var countDimension = 0
    var countInteger = 0
    var countMultiple = 0
    var countNotApplicable = 0
    var countNotAvailable = 0
    var countPartial = 0
    var countReal = 0
    var countString = 0
    var countUnit = 0
    var countVersion = 0


    val interpretations = pcm.getProducts.flatMap(_.getCells).map(c => Option(c.getInterpretation))
    for (interpretation <- interpretations) {
      interpretation match {
        case Some(value) => value match {
          case v: BooleanValue => countBoolean += 1
          case v: Conditional => countConditional += 1
          case v: DateValue => countDate += 1
          case v: Dimension => countDimension += 1
          case v: IntegerValue => countInteger += 1
          case v: Multiple => countMultiple += 1
          case v: NotApplicable => countNotApplicable += 1
          case v: NotAvailable => countNotAvailable += 1
          case v: Partial => countPartial += 1
          case v: RealValue => countReal += 1
          case v: StringValue => countString += 1
          case v: Unit => countUnit += 1
          case v: Version => countVersion += 1

        }
        case None => countNoInterpretation += 1
      }
    }


    ValueResult(countNoInterpretation,
      countBoolean,
      countConditional,
      countDate,
      countDimension,
      countInteger,
      countMultiple,
      countNotApplicable,
      countNotAvailable,
      countPartial,
      countReal,
      countString,
      countUnit,
      countVersion)
  }

}
