package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex
import ca.uwo.eng.sel.cepsim.sched.alloc.{UniformAllocationStrategy, WeightedAllocationStrategy, AllocationStrategy}

import java.util.{Map => JavaMap}

/** DefaultOpScheduleStrategy companion object. */
object DefaultOpScheduleStrategy {


  def apply(allocStrategy: AllocationStrategy) = new DefaultOpScheduleStrategy(allocStrategy)

  def uniform() = new DefaultOpScheduleStrategy(UniformAllocationStrategy())
  def weighted() = new DefaultOpScheduleStrategy(WeightedAllocationStrategy.apply(Map.empty[Vertex, Double].withDefaultValue(1.0)))


  def weighted(weights: Map[Vertex, Double]) = new DefaultOpScheduleStrategy(WeightedAllocationStrategy.apply(weights))
  def weighted(weights: JavaMap[Vertex, Double]) = new DefaultOpScheduleStrategy(WeightedAllocationStrategy.apply(weights))

}

/**
  * Default scheduling strategy. This strategy distributes the available instructions according to the
  * informed allocation strategy, and iterate through the vertex according to the order defined by the
  * placement class.
  * @param allocStrategy Strategy used to split instructions among all vertices from the placement.
  */
class DefaultOpScheduleStrategy(allocStrategy: AllocationStrategy) extends OpScheduleStrategy {

  // cache results - performance improvement
  var cachedResults: Map[(Double, Placement), Map[Vertex, Double]] = Map.empty


  override def allocate(instructions: Double, startTime: Double, capacity: Double, placement: Placement): Iterator[Action] =  {

    val instrPerOperator = cachedResults.get((instructions, placement)) match {
      case Some(result) => result
      case None         => {
        val result = allocStrategy.instructionsPerOperator(instructions, placement)
        cachedResults = cachedResults updated ((instructions, placement), result)
        result
      }
    }

    // build the return list
    var time = startTime
    placement.iterator.map((v) => {
      val start = time
      val end   = endTime(start, instrPerOperator(v), capacity)
      time = end
      ExecuteAction(v, start, end, instrPerOperator(v))
    })
  }

}

