package ca.uwo.eng.sel.cepsim.query

import scala.collection.immutable.TreeMap

/** Trait that represent vertices that have incoming edges. */
trait InputVertex extends Vertex  { this: Vertex =>

  /** Map representing the vertex input queues. */
  var inputQueues: Map[Vertex, Double] = TreeMap[Vertex, Double]()(Vertex.VertexIdOrdering)

  /** The maximum queue size. */
  val queueMaxSize: Int

  /**
    * Indicates if this vertex has bounded queues.
    * @return <code>true</code> if the vertex has bounded queues.
    */
  def isBounded() = queueMaxSize > 0

  /**
    * Initializes the input queues with a set of predecessors.
    * @param predecessors Set containing the vertex predecessors.
    */
  def initInputQueues(predecessors: Set[Vertex]) = {
    predecessors.foreach(addInputQueue(_))
  }

  /**
   * Add a new input queue for a new predecessor.
   * @param v New predecessor vertex.
   */
  def addInputQueue(v: Vertex) = {
    inputQueues = inputQueues + (v -> 0)
  }

  /** 
    * Gets the total number of events in all input queues.
    * @return total number of events in all input queues.
    */
  def totalInputEvents = Vertex.sumOfValues(inputQueues)

  /** 
    * Enqueue events into an input queue.
    * @param pred Predecessor that identifies the queue.
    * @param quantity Number of events to be enqueued.
    * @return Total number of events in the queue after the enqueuing.
    */
  def enqueueIntoInput(pred: Vertex, quantity: Double): Double = {
    inputQueues = inputQueues updated(pred, inputQueues(pred) + quantity)
    inputQueues(pred)
  }

  /**
    * Remove events from input queues.
    * @param pairs Pairs of vertex (that identifies an input queue) and the number of events to be dequeued.
    */
  protected def dequeueFromInput(pairs: (Vertex, Double)*) =
    pairs.foreach {(pair) =>
      inputQueues = inputQueues updated(pair._1, inputQueues(pair._1) - pair._2)
    }

  /**
    * Gets the set of vertex predecessors.
    * @return set containing all vertex predecessors.
    */
  override def predecessors: Set[OutputVertex] = queries.flatMap(_.predecessors(this))

}
