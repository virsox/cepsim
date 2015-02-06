package ca.uwo.eng.sel.cepsim.gen

import scala.concurrent.duration._

/**
  * Event generators.
  */
trait Generator {

  /** Current calculated average */
  protected var currentAvg = 0.0

  /** Number of invocations */
  protected var invocations = 0L

  /** Number of accumulated events that have not been processed. */
  protected var accumulated = 0.0

  /**
    * The average number of events generated per second.
    * @return average number of events generated per second.
    */
  def average: Double = currentAvg

  /**
   * Obtain the number of events that have not been processed.
   * @return the number of events that have not been processed.
   */
  def nonProcessed: Double = accumulated

  /**
   * Abstract method that should be overriden by subclasses with the generation logic.
   * @return The number of events generated events.
   */
  def doGenerate(): Double

  /** Simulation interval in milliseconds. */
  def samplingInterval: Long

  /**
   * Generates a number of events. It should be invoked at each simulation tick.
   * @param limit Limit in the number of generated events.
   * @return Number of events generated.
   */
  def generate(limit: Int = 10000): Double = {
    accumulated += doGenerate()

    // determine how many events should be returned
    val toReturn = accumulated.min(limit)
    accumulated -= toReturn

    // update average
    val sampleAvg = toReturn / (samplingInterval / 1000.0)
    currentAvg = ((invocations * currentAvg) + sampleAvg) / (invocations + 1)
    invocations = invocations + 1

    toReturn
  }



}
