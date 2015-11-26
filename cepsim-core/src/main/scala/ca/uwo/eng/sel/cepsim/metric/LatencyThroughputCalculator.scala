package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.history._
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._

import scala.collection.SortedMap

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
  var latencies: Map[Vertex, Vector[LatencyMetric]] = Map.empty withDefaultValue(Vector.empty)

  /** Map from event consumers to calculated throughputs. */
  var throughputs: Map[Vertex, Vector[ThroughputMetric]] = Map.empty withDefaultValue(Vector.empty)

  /** Number of existing paths from a consumer to each producer. */
  var pathsNo: Map[(EventConsumer, EventProducer), Int] = Map.empty

  /** Initial timestamp. */
  var startTime = 0.0

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
   * Initialize the metric calculator.
   * @param time Timestamp at which the cloudlet start its execution.
   */
  override def init(time: Double): Unit = startTime = time

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
    if (id == ThroughputMetric.ID) {
        throughputs(v).foldLeft(0.0)((acc, vectorEntry) => acc + vectorEntry.value) / throughputs(v).length

    } else if (id == LatencyMetric.ID) {
        val sums = latencies(v).foldLeft((0.0, 0.0))((acc, vectorEntry) =>
            (acc._1 + vectorEntry.quantity * vectorEntry.value, acc._2 + vectorEntry.quantity))
        sums._1 / sums._2

    } else {
      throw new IllegalArgumentException("Invalid Metric ID")
    }
  }

  def consolidateByMinute(id: String, v: Vertex): SortedMap[Int, Double] = {

    // TODO
    // There's only one ThroughputMetric object for each minute of the simulation, but there is
    // LatencyMetric object for each "Consumed" object processed at the update method
    // this need to be made consistent

    if (id == ThroughputMetric.ID) {
      SortedMap[Int, Double]() ++
        throughputs(v).groupBy((metric) => Math.floor(metric.time / 60.0).toInt).
                       map((mapEntry) => mapEntry._1 ->
                                         mapEntry._2.foldLeft(0.0)((acc, vectorEntry) => acc + vectorEntry.value) / 60.0)

    } else if (id == LatencyMetric.ID) {
      SortedMap[Int, Double]() ++
        latencies(v).groupBy((metric) => Math.floor(metric.time / 60000.0).toInt).
                     map((mapEntry) => {
                           val sums = mapEntry._2.foldLeft((0.0, 0.0))((acc, vectorEntry) =>
                             (acc._1 + vectorEntry.quantity * vectorEntry.value, acc._2 + vectorEntry.quantity))
                           mapEntry._1 -> (sums._1 / sums._2)
                        })

    } else {
      throw new IllegalArgumentException("Invalid Metric ID")
    }
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


    // calculate the total number of events processed
    val total = consumed.es.totals.foldLeft(0.0)((acc, e) => acc + (e._2 / pathsNo(consumer, e._1)))

    // at which simulation second this total should be added
    val second = Math.floor((consumed.at - startTime) / 1000.0).toInt

    // add the consumed events to an already existing ThroughputMetric object, or create a new one
    val consumerThroughput = throughputs(consumer)
    if ((!consumerThroughput.isEmpty) && (consumerThroughput.last.time == second)) {
      val last = consumerThroughput.last
      last.value = last.value + total
    } else {
      throughputs = throughputs updated (consumer, consumerThroughput :+ ThroughputMetric(consumer, second, total))
    }
  }


}
