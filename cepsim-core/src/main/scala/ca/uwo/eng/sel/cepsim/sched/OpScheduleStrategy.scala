package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/**
  * Strategy to distribute the available instructions to the placement vertices. It also defines the
  * order on which the vertices should be traversed.
  */
trait OpScheduleStrategy {

  // auxiliary function
  def instructionsPerMs(capacity: Double) = (capacity * 1000)
  def instructionsInMs(number: Double, capacity: Double) = number / instructionsPerMs(capacity)
  def endTime(startTime: Double, number: Double, capacity: Double) = startTime + instructionsInMs(number, capacity)

  /**
    * Allocates instructions to vertices from a placement.
    *
    * @param instructions Number of instructions to be allocated.
    * @param startTime The current simulation time (in milliseconds).
    * @param capacity The total processor capacity (in MIPS) that is allocated to this cloudlet.
    * @param placement Placement object encapsulating the vertices.
    * @return An iterator of Actions that must be executed by the simulation engine.
    */
  def allocate(instructions: Double, startTime: Double, capacity: Double, placement: Placement): Iterator[Action]
}
