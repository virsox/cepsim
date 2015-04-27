package ca.uwo.eng.sel.cepsim.sched

import java.util.{Map => JavaMap}

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex
import ca.uwo.eng.sel.cepsim.sched.alloc.{AllocationStrategy, UniformAllocationStrategy, WeightedAllocationStrategy}

import scala.collection.SortedSet
import scala.collection.immutable.TreeSet

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


  /**
    * Merge two list of actions. The list on the right cannot contain actions that are not instantaneous because
    * otherwise the resulting list would have a duration longer than the list on the left.
    *
    * @param executeActions First list (containing execute actions).
    * @param pendingActions Second list (containing enqueue actions).
    * @return A new list resulting from the merge of both parameters.
    */
  def merge(executeActions: Iterable[Action], pendingActions: Iterable[Action]): Iterable[Action] = {

    // micro optimization
    if (pendingActions.isEmpty) return executeActions

    def next(it: Iterator[Action]): Action = if (it.hasNext) it.next() else null

    val executeIt = executeActions.iterator
    val pendingIt = pendingActions.iterator

    var result = List.empty[Action]
    var executeElem: Action = next(executeIt)
    var pendingElem: Action = next(pendingIt)

    while ((executeElem != null) && (pendingElem != null)) {
      if (executeElem.before(pendingElem))  {
        result = result :+ executeElem
        executeElem = next(executeIt)

      } else if (executeElem.after(pendingElem)) {
        result = result :+ pendingElem
        pendingElem = next(pendingIt)

      } else if (executeElem.include(pendingElem)) {
        val(elem1, elem2) = executeElem.splitAt(pendingElem.from)
        result = result :+ elem1 :+ pendingElem :+ elem2
        executeElem = next(executeIt)
        pendingElem = next(pendingIt)

      } else {
        throw new IllegalStateException("Pending actions list contains non instantenous actions")
      }
    }

    // remaining elements
    if (executeElem != null) {
      do {
        result = result :+ executeElem
        executeElem = next(executeIt)
      } while (executeElem != null)
    }

    if (pendingElem != null) {
      do {
        result = result :+ pendingElem
        pendingElem = next(pendingIt)
      } while (pendingElem != null)
    }
    result
  }

  /**
    * Allocate instructions to the vertices according to the policy described in the class description.
    *
    * @param instructions Number of instructions to be allocated.
    * @param startTime The current simulation time (in milliseconds).
    * @param capacity The total processor capacity (in MIPS) that is allocated to this cloudlet.
    * @param placement Placement object encapsulating the vertices.
    * @param pendingActions Actions in the cloudlet that still need to be executed.
    * @return An iterator of Actions that must be executed by the simulation engine.
    */
  override def allocate(instructions: Double, startTime: Double, capacity: Double, placement: Placement,
                        pendingActions: SortedSet[Action] = TreeSet.empty): Iterator[Action] =  {

    val instrPerOperator = cachedResults.get((instructions, placement)) match {
      case Some(result) => result
      case None         => {
        val result = allocStrategy.instructionsPerOperator(instructions, placement)
        cachedResults = cachedResults updated ((instructions, placement), result)
        result
      }
    }

    // build the execute actions list
    var time = startTime
    val executeActions = placement.iterator.map((v) => {
      val start = time
      val end   = endTime(start, instrPerOperator(v), capacity)
      time = end
      ExecuteAction(v, start, end, instrPerOperator(v))
    }).toList

    val iterationEndTime = startTime + instructionsInMs(instructions, capacity)
    merge(executeActions,
          pendingActions.filter(_.to < iterationEndTime)).iterator
  }

}

