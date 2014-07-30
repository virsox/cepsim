package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.query.Query

class SingleVmOpPlacementStrategy(vmId: Long) extends OpPlacementStrategy {
  def execute(queries: Query*): Map[Query, List[Placement]] = {
    queries map { q => (q, List(Placement(q, vmId))) } toMap
  }
}