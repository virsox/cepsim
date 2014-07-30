package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy

import scala.concurrent.duration.Duration

class QueryCloudlet(interval: Duration, opSchedStrategy: OpScheduleStrategy) {


  var query: Query = null
  var currentPlacement: Placement = null

  def init(placement: Placement) = {
    currentPlacement = placement
    query = currentPlacement.query

    currentPlacement.foreach {(v) =>
      v.init(query)
    }
  }

  def run(availableInstructions: Double): Unit = {
    val instrPerVertex = opSchedStrategy.allocate(availableInstructions, currentPlacement)


    currentPlacement.foreach{(v) =>

      if (v.isInstanceOf[InputVertex]) {
        val predecessors = query.predecessors(v)
        predecessors.foreach{(pred) =>
          var events = 0
          pred match {
            case out: OutputVertex => {
              events = out.outputQueues(v)
              out.dequeueFromOutput((v, events))
            }
            case _ =>
          }
          v.asInstanceOf[InputVertex].enqueueIntoInput(pred, events)
        }
      }
      v.run(instrPerVertex(v))

    }
  }


}