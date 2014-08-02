package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.metric.History
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy

import scala.concurrent.duration.Duration


object QueryCloudlet {
  def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy, startTime: Double) =
    new QueryCloudlet(id, placement, opSchedStrategy, startTime)
}

class QueryCloudlet(val id: String, val placement: Placement, val opSchedStrategy: OpScheduleStrategy, val startTime: Double) {

  // a cloudlet should be  stateless
  // for each interval, a cloudlet will represent the execution of all queries allocated
  // to a VM. So, we may create another one for the next interval - and the state regarding the
  // query execution shouldn' be kept here


//  var query: Query = null
//  var currentPlacement: Placement = null
//  var history: History = null
//
//
//  def init(placement: Placement) = {
//    currentPlacement = placement
//    query = currentPlacement.query
//
//    currentPlacement.foreach {(v) =>
//      v.init(query)
//    }
//  }

  /** *
    *
    * @param instructions Number of instructions (in millions)
    * @return
    */
  def run(instructions: Double): History = {

    val availableInstructions = instructions * 1000000

    val instructionsPerMs = (placement.vm.mips * 1000)
    def totalMs(number: Double) = number / instructionsPerMs

    val instrPerVertex = opSchedStrategy.allocate(availableInstructions, placement)
    val query = placement.query
    val history = History()
    var time = startTime

    placement.foreach{(v) =>

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
      val events = v.run(instrPerVertex(v))
      history.log(id, time, v, events)
      time += totalMs(instrPerVertex(v))
    }
    history
  }




}