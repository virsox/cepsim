package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.history._
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._

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
   * Consolidates all the metric values that have been calculated for a specific vertex.
   * It calculates the average for latency, and simply select the last metric for throughput.
   * @param id Metric identifier.
   * @param v the specified vertex.
   * @return A single value that consolidates latency or throughput
   */
  override def consolidate(id: String, v: Vertex): Double = {
    if (id == LatencyMetric.ID) super.consolidate(id, v)
    else throughputs(v).last.value
  }

  /**
   * Method invoked to update the metrics calculation with new processing information.
   * @param event Object encapsulating some important event happened during the simulation.
   */
  override def update(event: SimEvent): Unit = { event match {
    // this calculator only needs to do something when it receives a consumed simulation event,
    // indicating the processing of a set of events had finished. All the remaining calculation is now
    // performed on the simulation logic / vertices.
    case c: Consumed  => updateWithConsumed(c)
    case _ =>
  }}


  /**
   * Update the metric calculation with a Consumed event.
   * @param consumed object encapsulating the event.
   */
  private def updateWithConsumed(consumed: Consumed) = {

    val consumer = consumed.v
    if (consumed.quantity > 0) {
      latencies = latencies updated (consumer,
        latencies(consumer) :+ LatencyMetric(consumed.v, consumed.es.ts, consumed.es.size, consumed.es.latency))
    }

    // --------- throughput code --------------------------------------------

    // updates the totalEvents map
    val sum = consumed.es.totals
    sum.foreach((e) => totalEvents = totalEvents updated (consumer,
      totalEvents(consumer) updated (e._1, totalEvents(consumer)(e._1) + e._2)
    ))

    // calculate the current metric value
    val total = totalEvents(consumer).foldLeft(0.0)((acc, e) => acc + (e._2 / pathsNo(consumer, e._1)))
    throughputs = throughputs updated (consumer, throughputs(consumer) :+ ThroughputMetric(consumer, consumed.at, total))
  }

}
