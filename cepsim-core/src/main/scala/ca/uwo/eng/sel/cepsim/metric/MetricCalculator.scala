package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.history.SimEvent
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

import scala.collection.SortedMap

/** Trait for metric calculators. */
trait MetricCalculator {

  /**
    * Gets the identifiers of calculated metrics.
    * @return calculator identifier.
    */
  def ids: Set[String]

  /**
    * Obtains the placement of which the metrics are being calculated.
    * @return placement of which the metrics are being calculated.
    */
  def placement: Placement

  /**
    * Initialize the metric calculator.
    * @param startTime Timestamp at which the cloudlet start its execution.
    */
  def init(startTime: Double)

  /**
    * Method invoked to update the metrics calculation with new processing information.
    * @param event Object encapsulating some important event happened during the simulation.
    */
  def update(event: SimEvent)

  /**
    * Obtains the values of a specific metric calculated for a specific vertex.
    * @param id Metric identifier.
    * @param v the specified vertex.
    * @return A list of metric values calculated for the vertex.
    */
  def results(id: String, v: Vertex): List[Metric]

  /**
    * Consolidates all the metric values that have been calculated for a specific vertex.
    * The default implementation simply calculates the average of these values.
    * @param id Metric identifier.
    * @param v the specified vertex.
    * @return A single value that consolidates the metric values.
    */
  def consolidate(id: String, v: Vertex): Double =
    results(id, v).foldLeft(0.0)((acc, metric) => acc + metric.value) / results(id, v).length

  def consolidateByMinute(id: String, v: Vertex): SortedMap[Int, Double]

}
