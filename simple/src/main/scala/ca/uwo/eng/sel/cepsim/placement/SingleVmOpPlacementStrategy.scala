package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.Vm
import ca.uwo.eng.sel.cepsim.query.Query

class SingleVmOpPlacementStrategy(vm: Vm) extends OpPlacementStrategy {
  def execute(queries: Query*): Map[Query, List[Placement]] = {
    queries map { q => (q, List(Placement(q, vm))) } toMap
  }
}