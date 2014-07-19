package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.query.Operator
import ca.uwo.eng.sel.cepsim.query.Vertex
import ca.uwo.eng.sel.cepsim.query.Query

object Placement {
  def apply(q: Query, vmId: Long): Placement =
    new Placement(q.vertices toList, vmId)
}

class Placement(vs: List[Vertex], vmId: Long) {  
	def vertices(): List[Vertex] = vs
	def vmId(): Long = vmId
}