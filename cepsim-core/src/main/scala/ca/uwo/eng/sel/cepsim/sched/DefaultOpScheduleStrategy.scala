package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex
import ca.uwo.eng.sel.cepsim.sched.alloc.{UniformAllocationStrategy, WeightedAllocationStrategy, AllocationStrategy}

/** DefaultOpScheduleStrategy companion object. */
object DefaultOpScheduleStrategy {

  def uniform() = new DefaultOpScheduleStrategy(UniformAllocationStrategy())
  def weighted() = new DefaultOpScheduleStrategy(WeightedAllocationStrategy.apply(Map.empty[Vertex, Double].withDefaultValue(1.0)))
  def weighted(weights: Map[Vertex, Double]) = new DefaultOpScheduleStrategy(WeightedAllocationStrategy.apply(weights))

}

/**
  * Default allocation strategy. This strategy distributes the available instructions equally
  * among the queries of the placement. For each query, the allocated instructions are distributed
  * among the vertices proportionally according to the instructions/event metric. For example, if an
  * operator x requires 10 instructions/event, and an operator y requires 5, then x will receive twice
  * more instructions.
  */
class DefaultOpScheduleStrategy(allocStrategy: AllocationStrategy) extends OpScheduleStrategy {

  override def allocate(instructions: Double, placement: Placement): Iterator[(Vertex, Double)] =  {
    val instrPerOperator = allocStrategy.instructionsPerOperator(instructions, placement)

    // build the return list
    placement.iterator.map((v) => (v, instrPerOperator(v)))
  }

}
