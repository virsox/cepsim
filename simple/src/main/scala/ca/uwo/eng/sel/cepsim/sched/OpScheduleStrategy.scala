package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/**
 * Created by virso on 2014-07-21.
 */
trait OpScheduleStrategy {
  def allocate(instructions: Double, placement: Placement): Map[Vertex, Double]
}
