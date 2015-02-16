package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{Vertex, EventProducer}

/**
 * Created by virso on 15-02-12.
 */
trait MetricCalculator {

  def id: String

  def placement: Placement

  def update(event: Event)

  def results: List[Metric]

  def consolidate: Double =
    results.foldLeft(0.0)((acc, metric) => acc + metric.value) / results.length



}
