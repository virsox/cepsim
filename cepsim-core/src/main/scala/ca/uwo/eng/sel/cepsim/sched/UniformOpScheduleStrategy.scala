package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex


/** UniformOpScheduleStrategy companion object */
object UniformOpScheduleStrategy {
  def apply() = new UniformOpScheduleStrategy()
}

/**
  * Scheduling strategy that allocates the same number of instructions for every operator in
  * the placement.
  */
class UniformOpScheduleStrategy extends OpScheduleStrategy {
  /**
   * Allocates instructions to vertices from a placement.
   *
   * @param instructions Number of instructions to be allocated.
   * @param placement Placement object encapsulating the vertices.
   * @return A list of pairs, in which the first element is a vertices and the second the number of
   *         instructions allocated to that vertex.
   */
  override def allocate(instructions: Double, placement: Placement): Iterator[(Vertex, Double)] = {
    val allocation = instructions / placement.vertices.size
    placement.iterator.map((_, allocation))
  }
}
