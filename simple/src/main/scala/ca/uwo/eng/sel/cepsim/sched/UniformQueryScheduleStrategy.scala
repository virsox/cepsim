package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.Host
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/**
 * Created by virso on 2014-07-21.
 */
class UniformQueryScheduleStrategy extends QueryScheduleStrategy {


  override def allocate(host: Host, placements: Set[Placement]): Map[Placement, Double] = {
    placements groupBy(_.vmId) flatMap { (mapEntry) =>
      mapEntry._2 map { (listEntry) =>
        (listEntry -> host.getVmMips(mapEntry._1) / mapEntry._2.size)
      }
    }
  }

}
