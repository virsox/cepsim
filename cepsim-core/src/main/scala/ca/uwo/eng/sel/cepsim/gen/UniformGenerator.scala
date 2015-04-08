package ca.uwo.eng.sel.cepsim.gen

/** UniformGenerator companion object. */
object UniformGenerator {
  def apply(rate: Double) = new UniformGenerator(rate)
}

/**
  * Uniform event generator. At each tick, generates the same number of events calculated as a function
  * of event generation rate and the sampling interval.
  *
  * @param rate Event generation rate in events / sec.
  */
class UniformGenerator(val rate: Double) extends Generator {

  override def doGenerate(interval: Double): Double = ((interval / 1000.0) * rate)

}