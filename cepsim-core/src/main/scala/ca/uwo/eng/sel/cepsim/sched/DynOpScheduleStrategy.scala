package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventProducer, InputVertex, Vertex}
import ca.uwo.eng.sel.cepsim.sched.alloc.AllocationStrategy

import scala.collection.SortedSet
import scala.collection.immutable.TreeSet

/** DynOpScheduleStrategy companion object. */
object DynOpScheduleStrategy {
  def apply(allocStrategy: AllocationStrategy) = new DynOpScheduleStrategy(allocStrategy)
}

/**
  * Schedule strategy that dynamically determines the next vertex to be processed. This strategy operates in
  * two or more rounds: in the first round, all operators receives the minimum between the number of instructions
  * required to process all the input queues and a maximum allocation value. The maximum allocation value
  * is determined by the allocation strategy informed as parameter. In the other rounds, the strategy iterates
  * through the operators and choose the next one which still has events in the input queue. This process is
  * repeated until all events have been processed OR there are no more instructions to be allocated.
  *
  * @param allocStrategy Strategy that determines the maximum number of instructions allocated to each vertex.
  */
class DynOpScheduleStrategy(allocStrategy: AllocationStrategy) extends OpScheduleStrategy {


  override def allocate(instructions: Double, startTime: Double, capacity: Double, placement: Placement,
                        pendingActions: SortedSet[Action] = TreeSet.empty): Iterator[Action] =
    new DynOpScheduleIterator(instructions, startTime, capacity, placement,
      pendingActions.filter(_.to < (startTime + instructionsInMs(instructions, capacity))))


  /**
    * Iterator returned by the strategy.
    * @param instructions Total number of instructions that can be allocated.
    * @param placement Placement object encapsulating the vertices.
    */
  class DynOpScheduleIterator(instructions: Double, startTime: Double, capacity: Double,
                              placement: Placement, pendingActions: SortedSet[Action])
    extends Iterator[Action] {

    /** Maximum number of instructions allocated to each vertex. */
    private val maxAllocation = allocStrategy.instructionsPerOperator(instructions, placement)

    /** Number of instructions still available. This number is updated at each iteration. */
    private var remainingInstructions = instructions

    /** List with all vertices in the iteration order determined by the placement. */
    private val vertices: List[Vertex] = placement.iterator.toList

    /** Current index in the vertices list - used in the second round. */
    private var currentIndex = 0

    /** Current start time. */
    private var currentTime = startTime

    /** List of actions that still need to be scheduled (initialized with all pending actions). */
    private var toBeScheduled: List[Action] = pendingActions.toList

    /**
      * Verify if the vertex can be allocated
      * @param v Vertex to be verified
      * @return true if the vertex can be allocated, false otherwise.
      */
    private def canAllocate(v: Vertex): Boolean =
      (instructionsNeeded(v) > 1) && (v.ipe < remainingInstructions)

    /**
      * Get the index of the next vertex to be allocated. Method used in the second round.
      * @return index of the next vertex to be allocated. -1 if there is no remaining vertex.
      */
    private def nextVertexIndex: Int = {
      var index = -1
      if (remainingInstructions > 0) {
        // search for the next vertex from the current index
        index = vertices.indexWhere(canAllocate(_), currentIndex)

        // if it hasn't found, search for the next vertex from the beginning
        if (index == -1) index = vertices.indexWhere(canAllocate(_), 0)
      }
      index
    }

    /**
     * Get the next vertex to be allocated. Method used in the second round.
     * @return next vertex to be allocated. null if there is no such vertex.
     */
    private def nextVertex(): Vertex = {
      val index = nextVertexIndex

      // if it hasn't found, then there are no more vertices to process
      if (index == -1) null
      else {
        currentIndex = (index + 1) % (vertices.size)
        vertices(index)
      }
    }

    /**
      * Calculate the total number of instructions needed to process the whole input queues.
      * @param v Vertex to which the calculation should be performed.
      * @return total number of instructions needed to process the whole input queues.
      */
    private def instructionsNeeded(v: Vertex): Double =
      v match {
        case in: InputVertex => in.totalInputEvents * in.ipe
        case _ => v.asInstanceOf[EventProducer].inputQueue * v.ipe
      }


    override def hasNext: Boolean = (!toBeScheduled.isEmpty) || (nextVertexIndex != -1)

    override def next(): Action = {

      // check for pending actions
      if (!toBeScheduled.isEmpty) {
        val (head, tail) = (toBeScheduled.head, toBeScheduled.tail)
        if ((head.from <= currentTime) || (nextVertexIndex == -1)) {
          toBeScheduled = tail
          if (currentTime < head.from)
            currentTime = head.to
          return head
        }
      }

      val v: Vertex = nextVertex()
      val allocation = instructionsNeeded(v).min(maxAllocation(v)).min(remainingInstructions)
      remainingInstructions -= allocation

      val start = currentTime
      val end   = endTime(start, allocation, capacity)
      currentTime = end
      val execute = ExecuteAction(v, start, end, allocation)

      // if there are pending actions that happens during the scheduled action,
      // then we need to split the action in two
      if ((!toBeScheduled.isEmpty) && (execute.include(toBeScheduled.head))) {
        val head = toBeScheduled.head
        val (p1, p2) = execute.splitAt(head.from)
        toBeScheduled = head +: (p2 +: toBeScheduled.tail)
        p1
      } else execute
    }

  }


}
