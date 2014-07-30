package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.Host
import ca.uwo.eng.sel.cepsim.placement.Placement

/** Strategy for scheduling queries into a VM.
  *
  *
  */
trait QueryScheduleStrategy {
  // TODO is it host information enough?
  def allocate(host: Host, placements: Set[Placement]): Map[Placement, Double]
}
