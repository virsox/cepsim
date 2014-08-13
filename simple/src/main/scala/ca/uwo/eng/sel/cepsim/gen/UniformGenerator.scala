package ca.uwo.eng.sel.cepsim.gen

import scala.concurrent.duration._

/** UniformGenerator companion object. */
object UniformGenerator {
  def apply(rate: Double, samplingInterval: Duration) =
    new UniformGenerator(rate, samplingInterval)
}

/**
  * Uniform event generator. At each tick, generates the same number of events calculated as a function
  * of event generation rate and the sampling interval.
  *
  * @param rate Event generation rate in events / sec.
  * @param samplingInterval Interval on which this generator is being sampled.
  */
class UniformGenerator(val rate: Double, samplingInterval: Duration)
  extends AbstractGenerator(samplingInterval) {

  /** Number of events per tick. */
  val tuplesPerInterval: Double = ((samplingInterval.toMillis / 1000.0) * rate)

  /** Keep track of partially generated events, in case the tuplesPerInterval is smaller than one. */
  var count: Double = 0
  
	
  override def doGenerate(): Int =
    if (tuplesPerInterval < 1) {
      count = count + tuplesPerInterval
      if (count >= 1) {
        count = 1 - count
        1        
      } else 0
    } else tuplesPerInterval.toInt


}