package ca.uwo.eng.sel.cepsim.gen


import scala.concurrent.duration._

/**
  * Abstract base class for other generators. It implements the logic to calculate the
  * average generation rate.
  * @param samplingInterval Interval on which this generator is being sampled.
  */
abstract class AbstractGenerator(val samplingInterval: Duration) extends Generator {

  /** Current calculated average */
  private var currentAvg = 0.0

  /** Number of invocations*/
  private var invocations = 0


  /**
    * Base implementation of the generate method that calculates the average generation rate.
    * @return Number of events generated.
    */
  override def generate(): Int = {
    val generated = doGenerate()

    val sampleAvg = generated / samplingInterval.toUnit(SECONDS)
    currentAvg = ((invocations * currentAvg) + sampleAvg) / (invocations + 1)
    invocations = invocations + 1

    generated
  }

  override def average: Double = currentAvg

  /**
    * Abstract method that should be overriden by subclasses with the generation logic.
    * @return The number of events generated events.
    */
  def doGenerate(): Int

}
