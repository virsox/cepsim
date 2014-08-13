package ca.uwo.eng.sel.cepsim.gen

/**
  * Event generators.
  */
trait Generator {

  /**
    * The average number of events generated per simulation tick.
    * @return average number of events generated per simulation tick.
    */
  def average: Double

  /**
   * Generates a number of events. It should be invoked at each simulation tick.
   * @return Number of events generated.
   */
  def generate(): Int

}