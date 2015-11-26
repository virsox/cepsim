package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, InputVertex, EventProducer, Vertex}


/*
  TODO probably a good idea to rename this metric to TotalEventMetric or something and create a new
  ThroughputMetric that depends on this one.
*/

/**
 * Throughput metric class. Actually, this metric is still not the throughput but the total number
 * of events that had to be produced in order to generate the events consumed by the vertex v. To obtain
 * the throughput, this calculated value must be divided by the period at which the vertex has been active.
 *
 * @param v EventConsumer of which the metric is calculated.
 * @param time Time of the calculation.
 * @param _value Metric value.
 */
case class ThroughputMetric(val v: Vertex, val time: Double, private var _value: Double) extends Metric {
  def value: Double = _value
  private [metric] def value_=(newValue: Double) = _value = newValue
}


/** ThroughputMetric companion object */
object ThroughputMetric {

  /** Throughput metric identifier - used to register with QueryCloudlet. */
  val ID = "THROUGHPUT_METRIC"

  /**
    * Obtains a calculator for the throughput metric.
    * @param placement Placement of which the metric will be calculated.
    * @return calculator for the throughput metric.
    */
  def calculator(placement: Placement) = new LatencyThroughputCalculator(placement)


}


