package ca.uwo.eng.sel.cepsim.gen

import scala.concurrent.duration._

class UniformGenerator(val rate: Double, val samplingInterval: Duration) {
  
  val tuplesPerInterval: Double = ((samplingInterval.toMillis / 1000.0) * rate)
  var count: Double = 0
  
	
  def generate(): Int =
    if (tuplesPerInterval < 1) {
      count = count + tuplesPerInterval
      if (count >= 1) {
        count = 1 - count
        1        
      } else 0
    } else tuplesPerInterval.toInt

}