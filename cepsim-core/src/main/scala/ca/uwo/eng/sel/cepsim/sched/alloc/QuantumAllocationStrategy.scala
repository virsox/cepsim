package ca.uwo.eng.sel.cepsim.sched.alloc

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex




/**
 * Created by virso on 2015-05-02.
 */
class QuantumAllocationStrategy(divisions: Int) extends AllocationStrategy {
  /**
   * Calculate the number os instructions to be allocated for each operator.
   *
   * @param instructions Number of instructions to be allocated.
   * @param placement Placement object encapsulating the vertices.
   * @return A map of vertices to the number of instructions allocated to that vertex.
   */
  override def instructionsPerOperator(instructions: Double, placement: Placement): Map[Vertex, Double] = {
    val allocation = (instructions / divisions) / placement.vertices.size
    placement.vertices.map((_, allocation)).toMap

  }
}
