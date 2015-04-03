package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._


/**
 * Encapsulates all information of an edge.
 * @param selectivity Edge selectivity.
 * @param eventSet EventSet that contains information about events currently in the queue associated with this edge.
 */
case class EdgeInfo(var selectivity: Double, var eventSet: EventSet) {

  /**
   * Dequeue events from the queue attached to this edge. It updates the EventSet associated with
   * the queue and return an EventSet with information about the dequeued events.
   * @param quantity Number of events to be dequeued.
   * @return EventSet containing information about the dequeued events.
   */
  def dequeue(quantity: Double): EventSet = eventSet.extract(quantity)

  /**
   * Enqueue events to the queue attached to this edge. It simply updates the EventSet
   * associated with the queue.
   * @param es EventSet containing information about the events being enqueued.
   */
  def enqueue(es: EventSet) = eventSet.add(es, selectivity)
}


/** LatencyThroughputCalculator companion object.  */
object LatencyThroughputCalculator {
  def apply(placement: Placement) = new LatencyThroughputCalculator(placement)
}


/**
  * This class can calculate both latency and throughput metrics. There is only one calculator
  * for both metrics because a lot of the bookeeping objects and logic are shared.
  *
  * @param placement Placement of which the metrics are calculated.
  */
class LatencyThroughputCalculator(val placement: Placement) extends MetricCalculator {

  /** Alias for a pair of vertices - used to locate an edge. */
  type EdgeKey = (Vertex, Vertex)  // (from, to)

  /** Map of all edges from the placement. */
  var edges: Map[EdgeKey, EdgeInfo] = Map.empty

  /**
   * Map of accumulated events - used for WindowedOperators. Each position of the list represents a
   * slot and contains an EventSet that summarizes information about the events on that slot.
   */
  var acc: Map[WindowedOperator, List[EventSet]] = Map.empty

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

        if (op.isInstanceOf[WindowedOperator]) {
          val windowedOp = op.asInstanceOf[WindowedOperator]
          acc = acc updated (windowedOp, List.fill(windowedOp.slots)(EventSet.withProducers(placement.producers)))
        }
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
    case p: Generated => updateWithGenerated(p)
    case p: Produced => updateWithProduced(p)
    case p: WindowProduced => updateWithWindowProduced(p)
    case a: WindowAccumulated => updateWithWindowAccumulated(a)
    case c: Consumed  => updateWithConsumed(c)
  }}

  /**
   * Update the metric calculation with a Generated event.
   * @param generated object encapsulating the event.
   */
  private def updateWithGenerated(generated: Generated) = {
    val key = (null, generated.v)
    edges(key) enqueue (EventSet(generated.quantity, generated.at, 0.0, Map(generated.v -> generated.quantity)))
  }

  /**
   * Update the metric calculation with a WindowProduced event.
   * @param produced object encapsulating the event.
   */
  private def updateWithWindowProduced(produced: WindowProduced) = {

    if (produced.quantity > 0) {

      val (sumSize, sumTs, sumLatency) = acc(produced.v).foldLeft((0.0, 0.0, 0.0))((acc, es) =>
        (acc._1 + es.size, acc._2 + (es.ts * es.size), acc._3 + (es.latency * es.size))
      )

      val eventSet = EventSet(produced.quantity, produced.at,
        (sumLatency / sumSize) + (produced.at - (sumTs / sumSize)),
        acc(produced.v)(produced.slot).totals
      )

      keys(produced.v).foreach((key) => edges(key) enqueue (eventSet) )

      val nextSlot = (produced.slot + 1) % produced.v.slots
      acc(produced.v)(nextSlot) reset()
    }
  }

  /**
   * Update the metric calculation with a WindowAccumulated event.
   * @param accumulated object encapsulating the event.
   */
  private def updateWithWindowAccumulated(accumulated: WindowAccumulated) = {

    val es = updatePredecessors(accumulated.v, accumulated.at, accumulated.quantity, accumulated.processed)
    acc(accumulated.v)(accumulated.slot) add es
  }

  /**
   * Update the metric calculation with a Produced event.
   * @param produced object encapsulating the event.
   */
  private def updateWithProduced(produced: Produced) = {
    val es = updatePredecessors(produced.v, produced.at, produced.quantity, produced.processed)
    keys(produced.v).foreach((key) => {
      edges(key) enqueue (es)
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
   * @param ts Timestamp of the invocation.
   * @param quantity Number of produced events.
   * @param predQueues Map from predecessor vertices to the number of events processed from them.
   * @return An EventSet encapsulting metrics from all incoming events.
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
