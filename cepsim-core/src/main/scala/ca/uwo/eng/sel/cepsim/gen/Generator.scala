package ca.uwo.eng.sel.cepsim.gen

/**
  * Event generators.
  */
trait Generator {

  /** Current calculated average (events per second) */
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
   * @param interval period of time in milliseconds from which the events are generated.
   * @return The number of generated events.
   */
  def doGenerate(interval: Double): Double

  /**
   * Generates a number of events. It should be invoked at the beginning of each iteration
   * at each simulation tick.
   * @param interval period of time in milliseconds from which the events are generated.
   * @param limit Limit in the number of generated events.
   * @return Number of events generated.
   */
  def generate(interval: Double, limit: Int = 10000000): Double = {
    accumulated += doGenerate(interval)

    // determine how many events should be returned
    val toReturn = accumulated.min(limit)
    accumulated -= toReturn

    // update average
    val sampleAvg = toReturn / (interval / 1000.0)
    currentAvg = ((invocations * currentAvg) + sampleAvg) / (invocations + 1)
    invocations = invocations + 1

    toReturn
  }



}
