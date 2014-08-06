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



  /** *
    *
    * @param instructions Number of instructions (in millions)
    * @return
    */
  def run(instructions: Double): History = {

    val availableInstructions = instructions * 1000000

    val instructionsPerMs = (placement.vm.mips * 1000)
    def totalMs(number: Double) = number / instructionsPerMs

    val verticesList = opSchedStrategy.allocate(availableInstructions, placement)
    val history = History()
    var time = startTime

    verticesList.foreach{(elem) =>

      val v: Vertex = elem._1
      if (v.isInstanceOf[InputVertex]) {

        // predecessors from all queries
        val predecessors = v.queries.flatMap(_.predecessors(v))
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
      val events = v.run(elem._2)
      history.log(id, time, v, events)
      time += totalMs(elem._2)
    }
    history
  }




}