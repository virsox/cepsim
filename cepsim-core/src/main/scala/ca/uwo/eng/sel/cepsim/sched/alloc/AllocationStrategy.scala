package ca.uwo.eng.sel.cepsim.sched.alloc

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/**
  * Trait for allocation strategies. These strategies split the number of available instructions among
  * the vertices from a placemenet.
  */
trait AllocationStrategy {
  /**
    * Calculate the number os instructions to be allocated for each operator.
    *
    * @param instructions Number of instructions to be allocated.
    * @param placement Placement object encapsulating the vertices.
    * @return A map of vertices to the number of instructions allocated to that vertex.
    */
  def instructionsPerOperator(instructions: Double, placement: Placement): Map[Vertex, Double];

}




