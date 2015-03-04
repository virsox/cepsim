package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/** Trait for metric calculators. */
trait MetricCalculator {

  /**
    * Gets the calculator identifier.
    * @return calculator identifier.
    */
  def id: String

  /**
    * Obtains the placement of which the metric is being calculated.
    * @return placement of which the metric is being calculated.
    */
  def placement: Placement

  /**
    * Method invoked to update the metric calculation with new processing information.
    * @param event Object encapsulating some important event happened during the simulation.
    */
  def update(event: Event)

  /**
    * Obtains the metric values calculated for a specific vertex.
    * @param v the specified vertex.
    * @return A list of metric values calculated for the vertex.
    */
  def results(v: Vertex): List[Metric]

  /**
    * Consolidates all the metric values that have been calculated for a specific vertex.
    * The default implementation simply calculates the average of these values.
    * @param v the specified vertex.
    * @return A single value that consolidates the metric values.
    */
  def consolidate(v: Vertex): Double =
    results(v).foldLeft(0.0)((acc, metric) => acc + metric.value) / results(v).length



}
