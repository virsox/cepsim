package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/**
  * Strategy to distribute the available instructions to the placement vertices. It also defines the
  * order on which the vertices should be traversed.
  */
trait OpScheduleStrategy {

  /**
    * Allocates instructions to vertices from a placement.
    *
    * @param instructions Number of instructions to be allocated.
    * @param placement Placement object encapsulating the vertices.
    * @return A list of pairs, in which the first element is a vertices and the second the number of
    *         instructions allocated to that vertex.
    */
  def allocate(instructions: Double, placement: Placement): List[(Vertex, Double)]
}
