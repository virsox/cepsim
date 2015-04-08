package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.query.Query

/***
  * Implementation of operator placement strategy that allocates all vertices to the
  * same virtual machine.
  * @param vmId Id of the VM to which the vertices should be allocated.
  */
class SingleVmOpPlacementStrategy(vmId: Int) extends OpPlacementStrategy {

  override def execute(queries: Query*): Set[Placement] = {
    Set(Placement(queries.flatMap{ _.vertices }.toSet, vmId))
  }
}