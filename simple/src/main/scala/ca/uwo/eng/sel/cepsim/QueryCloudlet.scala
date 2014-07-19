package ca.uwo.eng.sel.cepsim

import scala.concurrent.duration.Duration
import ca.uwo.eng.sel.cepsim.placement.Placement

class QueryCloudlet(val interval: Duration) {
	
  def setAvailableMips(mips: Double) = ???
  def setPlacement(placement: Placement) = ???
}