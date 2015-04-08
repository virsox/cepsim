package ca.uwo.eng.sel.cepsim.query

import scala.collection.immutable.TreeMap

/** Trait that represent vertices that have outgoing edges. */
trait OutputVertex extends Vertex {

  /** Map representing the vertex output queues. */
  var outputQueues: Map[Vertex, Double] = TreeMap[Vertex, Double]()(Vertex.VertexIdOrdering)

  /** Map with the selectivity associated with each successor vertex. */
  var selectivities: Map[Vertex, Double] = Map.empty

  /** Map with the limit size of each output queue. */
  var limits: Map[Vertex, Double] = Map.empty

  /**
    * Set containing all successors of this vertex.
    * @return Set containing all successors of this vertex.
    */
  override def successors: Set[InputVertex] = queries.flatMap(_.successors(this))

  /**
   * Initializes the output queues with a set of successors (assumes selectivity of 1.0 for all successors).
   * @param successors Set containing the vertex successors.
   */
  def initOutputQueues(successors: Set[Vertex]) = {
    successors.foreach(addOutputQueue(_))
  }

  /**
    * Adds a new output queue for a new successor.
    * @param v New successor vertex.
    * @param selectivity Edge selectivity.
    */
  def addOutputQueue(v: Vertex, selectivity: Double = 1.0) = {
    outputQueues = outputQueues + (v -> 0)
    selectivities = selectivities + (v -> selectivity)
    limits = limits + (v -> Int.MaxValue)
  }

  /**
    * Sets the limit of a specific output queue.
    * @param v Successor vertex that identifies the output queue.
    * @param limit The size limit.
    */
  def setLimit(v: Vertex, limit: Double) = {
    limits = limits updated (v, limit)
  }

  /**
    * Obtains the maximum number of events that can be produced by the vertex (it is calculated
    * as minimum among all limited output buffers taking the output selectivity into account).
    * @return maximum number of events that can be produced by the vertex.
    */
  def maximumNumberOfEvents: Double = {
    limits.map((elem) => (elem._1, elem._2 / selectivities(elem._1))).
                      foldLeft(Double.MaxValue)((acc, elem) => acc.min(elem._2))
  }

  /**
    * Remove events from the output queues.
    * @param pairs Pairs of vertex (that identifies an output queue) and the number of events to be dequeued.
    */
  def dequeueFromOutput(pairs: (Vertex, Double)*) =
    pairs.foreach {(pair) =>
      outputQueues = outputQueues updated(pair._1, outputQueues(pair._1) - pair._2)
    }

  /**
    * Send a number of events to all output queues.
    * @param quantity Number of events to be sent.
    * @return Map from successors to the number of events inserted into its corresponding queue.
    */
  def sendToAllOutputs(quantity: Double): Map[Vertex, Double] = {
    sendToOutputs(selectivities.mapValues((elem) => quantity))
  }

  /**
    * Send events to the output queues.
    * @param outputs Map from successors to the number of events to be sent.
    * @return Map from successors to the number of events inserted into its corresponding queue.
    */
  def sendToOutputs(outputs: Map[Vertex, Double]): Map[Vertex, Double] = {

    val withSelectivity = outputs.map{(elem) =>
      (elem._1, elem._2 * selectivities(elem._1))
    }

    outputQueues = outputQueues.map{(elem) =>
      (elem._1, elem._2 + withSelectivity.getOrElse(elem._1, 0.0))
    }
    withSelectivity
  }

}
