package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/**
 * Created by virso on 2014-07-23.
 */
class EvenOpScheduleStrategy extends OpScheduleStrategy {

  override def allocate(instructions: Double, placement: Placement): Map[Vertex, Double] =  {
    val total = placement.vertices.foldLeft(0.0){
      (sum, v) => sum + v.ipe
    }

    placement.vertices.map{(v) =>
      (v -> (v.ipe / total) * instructions)
    }.toMap

    //placement.vertices map ((_, mips / placement.vertices().size)) toMap
  }

}
