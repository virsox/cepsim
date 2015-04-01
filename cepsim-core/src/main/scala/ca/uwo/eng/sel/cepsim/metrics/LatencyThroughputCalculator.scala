package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{InputVertex, EventConsumer, EventProducer, Vertex}



case class EventSet(quantity: Double, ts: Double, latency: Double, totals: Map[EventProducer, Double])

/**
 * Encapsulates all information of an edge.
 * @param selectivity Edge selectivity.
 * @param size Current size of the queue attached to this edge.
 * @param timestamp Average timestamp at which currently enqueued events have arrived in the queue (ts_q).
 * @param latency Average latency of currently enqueued events (l_q).
 * @param totals Total number of events from each producer that originated the events currently
 *               in the queue (tl_q).
 */
case class EdgeInfo(var selectivity: Double, var size: Double, var timestamp: Double, var latency: Double,
               var totals: Map[EventProducer, Double]) {

  /**
   * Dequeue events from the queue attached to this edge. It updates the queue size and the totals map.
   * @param quantity Number of events to be dequeued.
   * @return Number of events from each producer that originated the dequeued events.
   */
  def dequeue(quantity: Double): Map[EventProducer, Double] = {

    // obtains the number of events from each producer that originated the events
    // it is simply calculated proportionally to the total number of events previously on the queue
    val totalFrom = totals.map((e) => (e._1, (quantity / size) * e._2))

    // update the totals map and size
    totals = totals.map((e) => (e._1, e._2 - totalFrom(e._1)))
    size -= quantity

    totalFrom
  }

  /**
   * Enqueue events to the queue attached to this edge. Updates the timestamp and latency currently
   * associated with the queues (formulas (4) and (5) from the paper), and also the totals map.
   *
   * @param quantity Number of events being enqueued.
   * @param newTimestamp Timestamp at which the enqueing is occurring.
   * @param newLatency Average latency of events being enqueued.
   * @param addToTotal Number of events to be added to totals for each producer.
   */
  def enqueue(es: EventSet) = {
    if (es.quantity > 0) {
      var newQuantity = selectivity * es.quantity
      timestamp = ((size * timestamp) + (newQuantity * es.ts))      / (size + newQuantity)
      latency   = ((size * latency)   + (newQuantity * es.latency)) / (size + newQuantity)
      size     += newQuantity
    }
    totals = totals map((e) => (e._1, e._2 + es.totals.getOrElse(e._1, 0.0)))
  }

  /** Reset queue attributes.  */
  def reset() = {
    size = 0; timestamp = 0; latency = 0;
    totals = totals map((e) => (e._1, 0.0))
  }

  override def toString(): String =
    s"(size = $size, timestamp=$timestamp, latency=$latency, totals = $totals)"

}


object LatencyThroughputCalculator {
  def apply(placement: Placement) = new LatencyThroughputCalculator(placement)
}

/**
 * Created by virso on 15-03-31.
 */
class LatencyThroughputCalculator(val placement: Placement) extends MetricCalculator {

  /** Alias for a pair of vertices - used to locate an edge. */
  type EdgeKey = (Vertex, Vertex)  // (from, to)

  /** Map of all edges from the placement. */
  var edges: Map[EdgeKey, EdgeInfo] = Map.empty

  /** Map of accumululated events - used for WindowedOperators. */
  var acc: Map[EdgeKey, EdgeInfo] = Map.empty

  /** Map from a vertex to all its outgoing edge keys. */
  var keys: Map[Vertex, Set[EdgeKey]] = Map.empty withDefaultValue(Set.empty)

  /** Map from event consumers to calculated latencies.  */
  var latencies: Map[Vertex, Vector[Metric]] = Map.empty withDefaultValue(Vector.empty)

  /** Map from event consumers to calculated throughputs. */
  var throughputs: Map[Vertex, Vector[Metric]] = Map.empty withDefaultValue(Vector.empty[Metric])

  /** Number of existing paths from a consumer to each producer. */
  var pathsNo: Map[(EventConsumer, EventProducer), Int] = Map.empty

  /**
   * For each consumer, it keeps track of the total number of events from each producer
   * that had to be generated in order to originate the events consumed.
   */
  var totalEvents: Map[EventConsumer, Map[EventProducer, Double]] = placement.consumers.map((e) =>
    (e, placement.producers.map((_, 0.0)).toMap)).toMap


  // initialize attributes
  placement.iterator.foreach((v) => { v match {
    case prod: EventProducer => {
      val key = (null, prod)
      edges = edges updated (key, new EdgeInfo(1.0, 0.0, 0.0, 0.0, Map(prod -> 0.0)))
    }
    case op: InputVertex => {
      op.predecessors.foreach((pred) => {
        val key = (pred, op)
        val emptyProducersMap = placement.producers.map((_, 0.0)).toMap

        keys = keys updated (pred, keys(pred) + key)
        edges = edges updated (key, new EdgeInfo(pred.selectivities(op), 0.0, 0.0, 0.0, emptyProducersMap))
        acc = acc updated (key, new EdgeInfo(1.0, 0.0, 0.0, 0.0, emptyProducersMap))
      })
    }
  }})

  // initialize the pathsNo map
  placement.consumers.foreach((consumer) => {
    consumer.queries.foreach((query) => {
      query.pathsToProducers(consumer).foreach((path) => {
        val key = (consumer, path.producer)
        pathsNo = pathsNo updated (key, pathsNo.getOrElse(key, 0) + 1)
      })
    })
  })

  /**
   * Gets the identifiers of calculated metrics.
   * @return calculator identifier.
   */
  override def ids: Set[String] = Set(LatencyMetric.ID, ThroughputMetric.ID)

  /**
   * Obtains the values of a specific metric calculated for a specific vertex.
   * @param id Metric identifier.
   * @param v the specified vertex.
   * @return A list of metric values calculated for the vertex.
   */
  override def results(id: String, v: Vertex): List[Metric] =
    if (id == LatencyMetric.ID) latencies(v).toList else throughputs(v).toList

  /**
   * Method invoked to update the metrics calculation with new processing information.
   * @param event Object encapsulating some important event happened during the simulation.
   */
  override def update(event: Event): Unit = { event match {
    case p: Produced => updateWithProduced(p)
    case p: Processed => updateWithProcessed(p)
    case c: Consumed  => updateWithConsumed(c)
  }}

  /**
   * Update the metric calculation with a Produced event.
   * @param produced object encapsulating the event.
   */
  private def updateWithProduced(produced: Produced) = {
    val key = (null, produced.v)
    edges(key) enqueue (EventSet(produced.quantity, produced.at, 0.0, Map(produced.v -> produced.quantity)))
  }

  /**
   * Update the metric calculation with a Processed event.
   * @param processed object encapsulating the event.
   */
  private def updateWithProcessed(processed: Processed) = {

    val es = updatePredecessors(processed.v, processed.at, processed.quantity, processed.processed)

    keys(processed.v).foreach((key) => {
      //edges(key).enqueue(processed.quantity, sum)

      // Window operator - accumulating
      if (processed.quantity == 0) {
        acc(key) enqueue (es)

      } else {
        if (acc(key).size > 0) {
          val accEntry = acc(key)

          // still accumulates for this iteration
          accEntry enqueue (es)

          // enqueue
          edges(key) enqueue (EventSet(processed.quantity, processed.at,
              accEntry.latency + (processed.at - accEntry.timestamp), accEntry.totals))
          accEntry reset()

        } else {
          edges(key) enqueue (es)
        }
      }


    })


  }

  /**
   * Update the metric calculation with a Consumed event.
   * @param consumed object encapsulating the event.
   */
  private def updateWithConsumed(consumed: Consumed) = {
    val es = updatePredecessors(consumed.v, consumed.at, consumed.quantity, consumed.processed)
    val consumer = consumed.v

    if (consumed.quantity > 0) {
      latencies = latencies updated (consumer,
        latencies(consumer) :+ LatencyMetric(consumed.v, consumed.at, consumed.quantity, es.latency))
    }


    // --------- throughput code --------------------------------------------

    // updates the totalEvents map
    val sum = es.totals
    sum.foreach((e) => totalEvents = totalEvents updated (consumer,
      totalEvents(consumer) updated (e._1, totalEvents(consumer)(e._1) + e._2)
    ))

    // calculate the current metric value
    val total = totalEvents(consumer).foldLeft(0.0)((acc, e) => acc + (e._2 / pathsNo(consumer, e._1)))
    throughputs = throughputs updated (consumer, throughputs(consumer) :+ ThroughputMetric(consumer, consumed.at, total))

  }

  /**
   * Auxiliary method used to update the predecessors queues.
   *
   * @param v vertex being processed.
   * @param quantity Number of events generated by the vertex.
   * @param predQueues Map of predecessors and number of events processed from each one.
   * @return A 4-tuple consisting of:
   *         1) the average timestamp;
   *         2) average latency;
   *         3) total number of events processed from the predecessors
   *         5) number of events from each producer needed to generate the vertex output.
   */
  private def updatePredecessors(v: Vertex, ts: Double, quantity: Double, predQueues: Map[Vertex, Double]): EventSet = {

    var timestampSum = 0.0
    var latencySum = 0.0
    var quantitySum = 0.0
    var eventsSum = Map.empty[EventProducer, Double]

    // event producer
    if (predQueues.isEmpty) {
      val key = (null, v)

      quantitySum  = quantity
      timestampSum = edges(key).timestamp * quantity
      latencySum   = 0.0
      eventsSum = edges(key).dequeue(quantitySum)

    } else {
      predQueues.foreach((entry) => {
        val key = (entry._1, v)

        quantitySum += entry._2
        timestampSum += (edges(key).timestamp * entry._2)
        latencySum += (edges(key).latency * entry._2)

        if (entry._2 > 0) {
          val totalFromProducers = edges((entry._1, v)).dequeue(entry._2)

          // sum all these totals
          totalFromProducers.foreach((e) => {
            eventsSum = eventsSum updated (e._1, eventsSum.getOrElse(e._1, 0.0) + e._2)
          })
        }
      })
    }
    if (quantitySum == 0) EventSet(0.0, 0.0, 0.0, eventsSum)
    else EventSet(quantitySum, ts,
                  (latencySum / quantitySum) + ts - (timestampSum / quantitySum), eventsSum)
  }




}
