package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.Vm
import ca.uwo.eng.sel.cepsim.query.Query

/***
  * Implementation of operator placement strategy that allocates all vertices to the
  * same virtual machine.
  * @param vm VM to which the vertices should be allocated.
  */
class SingleVmOpPlacementStrategy(vm: Vm) extends OpPlacementStrategy {

  override def execute(queries: Query*): Map[Query, List[Placement]] = {
    queries map { q => (q, List(Placement(q, vm))) } toMap
  }
}