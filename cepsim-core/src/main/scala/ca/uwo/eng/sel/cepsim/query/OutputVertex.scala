package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.event.{EventSetQueue, EventSet}

/** Trait that represent vertices that have outgoing edges. */
trait OutputVertex extends Vertex {

  /** Event sets that represent the output queues. */
  var outputEventQueues: Map[Vertex, EventSetQueue] = Map.empty

  /** Map with the selectivity associated with each successor vertex. */
  var selectivities: Map[Vertex, Double] = Map.empty

  /** Map with the limit size of each output queue. */
  var limits: Map[Vertex, Double] = Map.empty

  /**
    * Set containing all successors of this vertex.
    * @return Set containing all successors of this vertex.
    */
  override def successors: Set[InputVertex] = outputEventQueues.keySet.collect { case iv: InputVertex => iv }

  /**
    * Adds a new output queue for a new successor.
    * @param v New successor vertex.
    * @param selectivity Edge selectivity.
    */
  def addOutputQueue(v: InputVertex, selectivity: Double = 1.0) = {
    outputEventQueues = outputEventQueues + (v -> EventSetQueue())
    selectivities = selectivities + (v -> selectivity)
    limits = limits + (v -> (if (v.queueMaxSize == 0) Long.MaxValue else v.queueMaxSize))
  }

  /**
    * Obtains the number of events in a output queue.
    * @param v Successor vertex to which the output queue is associated.
    * @return Number of events on the informed queueu.
    */
  def outputQueues(v: Vertex): Double = outputEventQueues(v).size

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
    limits.map((elem) => (elem._1,
                         (elem._2 / selectivities(elem._1)) - (outputEventQueues(elem._1).size / selectivities(elem._1)))
              ).foldLeft(Double.MaxValue)((acc, elem) => acc.min(elem._2))
  }

  /**
    * Dequeue a number of events from an output queue.
    *
    * @param v Vertex used to located the output queue.
    * @param quantity Number of events to be dequeued.
    * @return EventSet encapsulating the dequeued events.
    */
  def dequeueFromOutput(v: Vertex, quantity: Double): EventSet = outputEventQueues(v).dequeue(quantity)

  /**
    * Send an event set to all output queues.
    * @param es Event set to be sent.
    */
  def sendToAllOutputs(es: EventSet): Unit =
    outputEventQueues.foreach((elem) => elem._2.enqueue(es, selectivities(elem._1)))

}
