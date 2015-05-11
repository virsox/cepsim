package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex
import ca.uwo.eng.sel.cepsim.sched.alloc.AllocationStrategy

import scala.collection.SortedSet
import scala.collection.immutable.TreeSet


object AltDynOpScheduleStrategy {
  def apply(allocStrategy: AllocationStrategy) = new AltDynOpScheduleStrategy(allocStrategy)

}

/**
 * Created by virso on 2015-05-09.
 */
class AltDynOpScheduleStrategy(allocStrategy: AllocationStrategy) extends OpScheduleStrategy {
  import OpScheduleStrategy._

  override def allocate(instructions: Double, startTime: Double, capacity: Double, placement: Placement,
                        pendingActions: SortedSet[Action] = TreeSet.empty): Iterator[Action] =
    new AltDynOpScheduleIterator(instructions, startTime, capacity, placement, pendingActions)

  // -----------------------------------------------------------


  class AltDynOpScheduleIterator(instructions: Double, startTime: Double, capacity: Double,
                              placement: Placement, pendingActions: SortedSet[Action])
    extends Iterator[Action] {

    /** Maximum number of instructions allocated to each vertex. */
    private var maxAllocation = allocStrategy.instructionsPerOperator(instructions, placement)

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
     * Get the index of the next vertex to be allocated. Method used in the second round.
     * @return index of the next vertex to be allocated. -1 if there is no remaining vertex.
     */
    private def nextVertexIndex: Int = {
      var index = -1
      if (remainingInstructions > 0) {
        // search for the next vertex from the current index
        index = vertices.indexWhere(_.needsAllocation, currentIndex)

        // if it hasn't found, search for the next vertex from the beginning
        if (index == -1) {
          index = vertices.indexWhere(_.needsAllocation, 0)
        }

      }
      index
    }

    var count = 0

    /**
     * Get the next vertex to be allocated. Method used in the second round.
     * @return next vertex to be allocated. null if there is no such vertex.
     */
    private def nextVertex(): Vertex = {
      val index = nextVertexIndex

      // if it hasn't found, then there are no more vertices to process
      if (index == -1) null
      else {
        // the index wrapped around the vertices array
        if (((currentIndex == 0 && index > currentIndex) || (currentIndex > index)) && (count < 2)) {
          val remaining = vertices.length - index

          // max instructions is equally divided among remaining vertices
          (index until vertices.length).foreach((i) => {
            maxAllocation = maxAllocation updated (vertices(i), remainingInstructions / remaining)
          })
          count += 1
        }
        currentIndex = (index + 1) % (vertices.size)
        vertices(index)
      }
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
      val allocation = v.instructionsNeeded.min(maxAllocation(v)).min(remainingInstructions)
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
