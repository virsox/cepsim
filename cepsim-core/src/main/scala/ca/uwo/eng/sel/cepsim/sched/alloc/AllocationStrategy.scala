package ca.uwo.eng.sel.cepsim.sched.alloc

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/**
 * Created by virso on 15-02-04.
 */
trait AllocationStrategy {
  /**
   * Calculate the number os instructions to be allocated for each operator. The calculation is made
   * according to the rules described in the DefaultOpScheduleStrategy class.
   *
   * @param instructions Number of instructions to be allocated.
   * @param placement Placement object encapsulating the vertices.
   * @return A map of vertices to the number of instructions allocated to that vertex.
   */
  def instructionsPerOperator(instructions: Double, placement: Placement): Map[Vertex, Double];

}




