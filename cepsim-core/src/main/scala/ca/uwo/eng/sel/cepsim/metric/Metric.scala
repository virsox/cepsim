package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.query.Vertex

/** Trait for metrics. */
trait Metric {

  /** Vertex to which the metric refers. */
  def v: Vertex

  /** Time at which the metric has been generated. */
  def time: Double

  /** Metric value. */
  def value: Double
}
