package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{InputVertex, EventConsumer, EventProducer, Vertex}



object EventSet {
  def empty(): EventSet = EventSet(0.0, 0.0, 0.0, Map.empty[EventProducer, Double] withDefaultValue(0.0))
  def withProducers(producers: Set[EventProducer]): EventSet = EventSet(0.0, 0.0, 0.0, producers.map((_, 0.0)).toMap)
}

case class EventSet(var size: Double, var ts: Double, var latency: Double, var totals: Map[EventProducer, Double]) {

  def add(es: EventSet, selectivity: Double = 1.0): Unit = {
    val newQuantity = selectivity * es.size

    if (newQuantity > 0) {
      ts = ((size * ts) + (newQuantity * es.ts)) / (size + newQuantity)
      latency = ((size * latency) + (newQuantity * es.latency)) / (size + newQuantity)
      size    = size + newQuantity
    }
    totals = totals.map((e) => (e._1, e._2 + es.totals.getOrElse(e._1, 0.0))) ++ (es.totals -- totals.keys)
  }


  def extract(quantity: Double): EventSet = {
    // obtains the number of events from each producer that originated the events
    // it is simply calculated proportionally to the total number of events previously on the queue
    val totalFrom = totals.map((e) => (e._1, if (size == 0) 0.0 else (quantity / size) * e._2))

    // update the totals map and size
    totals = totals.map((e) => (e._1, e._2 - totalFrom(e._1)))
    size -= quantity

    EventSet(quantity, ts, latency, totalFrom)
  }

  /** Reset queue attributes.  */
  def reset() = {
    size = 0; ts = 0; latency = 0;
    totals = totals map((e) => (e._1, 0.0))
  }

  override def toString(): String =
    s"(size = $size, timestamp=$ts, latency=$latency, totals = $totals)"

}

/**
 * Encapsulates all information of an edge.
 * @param selectivity Edge selectivity.
 */
case class EdgeInfo(var selectivity: Double, var eventSet: EventSet) {

  /**
   * Dequeue events from the queue attached to this edge. It updates the queue size and the totals map.
   * @param quantity Number of events to be dequeued.
   * @return Number of events from each producer that originated the dequeued events.
   */
  def dequeue(quantity: Double): EventSet = eventSet.extract(quantity)


  /**
   * Enqueue events to the queue attached to this edge. Updates the timestamp and latency currently
   * associated with the queues (formulas (4) and (5) from the paper), and also the totals map.
   *
   */
  def enqueue(es: EventSet) = eventSet.add(es, selectivity)

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
      edges = edges updated (key, new EdgeInfo(1.0, EventSet.withProducers(Set(prod))))
    }
    case op: InputVertex => {
      op.predecessors.foreach((pred) => {
        val key = (pred, op)

        keys = keys updated (pred, keys(pred) + key)
        edges = edges updated (key, new EdgeInfo(pred.selectivities(op), EventSet.withProducers(placement.producers)))
        acc = acc updated (key, new EdgeInfo(1.0, EventSet.withProducers(placement.producers)))
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
        if (acc(key).eventSet.size > 0) {

          // still accumulates for this iteration
          acc(key) enqueue (es)
          val accEventSet = acc(key).eventSet

          // enqueue
          edges(key) enqueue (EventSet(processed.quantity, processed.at,
            accEventSet.latency + (processed.at - accEventSet.ts), accEventSet.totals))

          accEventSet reset()

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

    var es: EventSet = EventSet.empty()

    // event producer
    if (predQueues.isEmpty) {
      val key = (null, v)
      es = edges(key).dequeue(quantity)
    } else {
      predQueues.foreach((entry) => {
        val key = (entry._1, v)
        es.add(edges(key).dequeue(entry._2))
      })
    }

    EventSet(es.size, ts, es.latency + ts - es.ts, es.totals)
  }




}
