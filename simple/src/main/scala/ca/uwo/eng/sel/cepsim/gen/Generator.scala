package ca.uwo.eng.sel.cepsim.gen

/**
  * Event generators.
  */
trait Generator {
  /**
    * Generates a number of events. It should be invoked at each simulation tick.
    * @return Number of events generated.
    */
  def generate(): Int
}