package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.metric.History
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy

import scala.concurrent.duration.Duration


object QueryCloudlet {
  def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy) = //, startTime: Double) =
    new QueryCloudlet(id, placement, opSchedStrategy) //, startTime)
}

class QueryCloudlet(val id: String, val placement: Placement, val opSchedStrategy: OpScheduleStrategy) { //, val startTime: Double) {

  // a cloudlet should be  stateless
  // for each interval, a cloudlet will represent the execution of all queries allocated
  // to a VM. So, we may create another one for the next interval - and the state regarding the
  // query execution shouldn' be kept here



  /**
    *
    * @param instructions Number of instructions that can be used in this simulation tick.
    * @param startTime The current simulation time.
    * @param capacity The total processor capacity (in MIPS) that is allocated to this cloudlet.
    * @return
    */
  def run(instructions: Double, startTime: Double, capacity: Double): History = {
    val history = History()
    if (instructions > 0) {
      
      val availableInstructions = instructions 
      val instructionsPerMs = (capacity * 1000)
      def totalMs(number: Double) = number / instructionsPerMs

      // generate the events before calling the scheduling strategy
      // in theory this enables more complex strategies that consider the number of
      // events to be consumed
      placement.producers foreach(_.generate())

      val verticesList = opSchedStrategy.allocate(availableInstructions, placement)    
      var time = startTime

      verticesList.foreach{(elem) =>

        val v: Vertex = elem._1
        var processedEvents = 0
        if (v.isInstanceOf[InputVertex]) {

          val iv = v.asInstanceOf[InputVertex]

          // predecessors from all queries
          val predecessors = iv.queries.flatMap(_.predecessors(v))
          predecessors.foreach{(pred) =>
            val events = pred.outputQueues(iv)
            pred.dequeueFromOutput((iv, events))
            iv.enqueueIntoInput(pred, events)
          }
          processedEvents = iv.run(elem._2)

          if (iv.isBounded()) {
            predecessors.foreach {(pred) =>
              pred.setLimit(iv, iv.queueMaxSize - iv.inputQueues(pred))
            }
          }

        } else {
          processedEvents = v.run(elem._2)
        }

        history.log(id, time, v, processedEvents)
        time += totalMs(elem._2)
      }      
    } 
    
    history
  }




}