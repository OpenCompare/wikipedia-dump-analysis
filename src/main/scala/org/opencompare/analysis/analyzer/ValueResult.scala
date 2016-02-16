package org.opencompare.analysis.analyzer

/**
  * Created by gbecan on 16/02/16.
  */
case class ValueResult(
                        countNoInterpretation: Int,
                        countBoolean: Int,
                        countConditional: Int,
                        countDate: Int,
                        countDimension: Int,
                        countInteger: Int,
                        countMultiple: Int,
                        countNotApplicable: Int,
                        countNotAvailable: Int,
                        countPartial: Int,
                        countReal: Int,
                        countString: Int,
                        countUnit: Int,
                        countVersion: Int)
