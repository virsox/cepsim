package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.query.Vertex

/**
 * Created by virso on 15-02-14.
 */
trait Metric {
  def v: Vertex
  def time: Double
  def value: Double
}
