package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{InputVertex, EventProducer, Vertex}

/** *
  * Latency metric class.
  * @param v EventConsumer of which the metric is calculated.
  * @param time Time of the calculation.
  * @param quantity Quantity of events to which the calculated metric refers.
  * @param value Calculated latency.
  */
case class LatencyMetric(val v: Vertex, val time: Double, val quantity: Double, val value: Double) extends Metric

/** LatencyMetric companion object. */
object LatencyMetric {

  /** Latency metric identifier - used to register with QueryCloudlet. */
  val ID = "LATENCY_METRIC"

  /**
   * Obtains a calculator for the latency metric.
   * @param placement Placement of which the metric will be calculated.
   * @return calculator for the latency metric.
   */
  def calculator(placement: Placement) = new LatencyThroughputCalculator(placement)

}


