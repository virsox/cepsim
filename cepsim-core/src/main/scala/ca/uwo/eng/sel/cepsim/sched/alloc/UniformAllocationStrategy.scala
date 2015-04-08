package ca.uwo.eng.sel.cepsim.sched.alloc

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/** UniformAllocationStrategy companion object. */
object UniformAllocationStrategy {
  def apply() = new UniformAllocationStrategy()
}

/**
  * Allocation strategy that uniformly distributes the available instructions among the placement vertices.
  */
class UniformAllocationStrategy extends AllocationStrategy {
  /**
    * Calculate the number os instructions to be allocated for each operator. This strategy allocates
    * the same number of instructions for each vertex.
    *
    * @param instructions Number of instructions to be allocated.
    * @param placement Placement object encapsulating the vertices.
    * @return A map of vertices to the number of instructions allocated to that vertex.
    */
  override def instructionsPerOperator(instructions: Double, placement: Placement): Map[Vertex, Double] = {
    val allocation = instructions / placement.vertices.size
    placement.vertices.map((_, allocation)).toMap
  }

}
