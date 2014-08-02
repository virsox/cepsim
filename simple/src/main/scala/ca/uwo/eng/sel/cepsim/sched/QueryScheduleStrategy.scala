package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.Host
import ca.uwo.eng.sel.cepsim.placement.Placement

/** Strategy for scheduling queries into a VM.
  *
  *
  */
trait QueryScheduleStrategy {
  def allocate(placements: Set[Placement]): Map[Placement, Double]
}
