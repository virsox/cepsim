package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventProducer, InputVertex, Vertex}

/** DynamicOpScheduleStrategy companion object. */
object DynamicOpScheduleStrategy {
  def apply() = new DynamicOpScheduleStrategy()
}

/**
  * Schedule strategy that dynamically determines the next vertex to be processed. This strategy operates in
  * two or more rounds: in the first round, all operators receives the minimum between the number of instructions
  * required to process all the input queues AND a maximum allocation value. The maximum allocation value
  * is determined by dividing the total number of available instructions by the total number of vertices.
  * In the other rounds, the strategy iterates through the operators and choose the next one which still
  * has events in the input queue. This process is repeated until all events have been processed OR there are
  * no more instructions to be allocated.
  */
class DynamicOpScheduleStrategy extends OpScheduleStrategy {

  override def allocate(instructions: Double, placement: Placement): Iterator[(Vertex, Double)] =
    new DynamicOpScheduleIterator(instructions, placement)

  /**
    * Iterator returned by the strategy.
    * @param instructions Total number of instructions that can be allocated.
    * @param placement Placement object encapsulating the vertices.
    */
  class DynamicOpScheduleIterator(instructions: Double, placement: Placement) extends Iterator[(Vertex, Double)] {

    /** Maximum number of instructions allocated to each vertex. */
    private val maxAllocation = instructions / placement.vertices.size

    /** Number of instructions still available. This number is updated at each iteration. */
    private var remainingInstructions = instructions

    /** Iterator used in the first round. */
    private val firstRoundIt = placement.iterator

    /** List with all vertices in the iteration order determined by the placement. */
    private val vertices: List[Vertex] = placement.iterator.toList

    /** Current index in the vertices list - used in the second round. */
    private var currentIndex = 0

    /**
      * Verify if the vertex can be allocated
      * @param v Vertex to be verified
      * @return true if the vertex can be allocated, false otherwise.
      */
    private def canAllocate(v: Vertex): Boolean =
      (instructionsNeeded(v) > 0) && (v.ipe < remainingInstructions)

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


    override def hasNext: Boolean = if (!firstRoundIt.hasNext) (nextVertexIndex != -1) else true

    override def next(): (Vertex, Double) = {
      var v: Vertex = null

      if (firstRoundIt.hasNext) v = firstRoundIt.next()
      else v = nextVertex()

      val allocation = instructionsNeeded(v).min(maxAllocation).min(remainingInstructions)
      remainingInstructions -= allocation

      (v, allocation)
    }

  }

}
