package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.metric.History
import ca.uwo.eng.sel.cepsim.metric.History.Entry
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy

import scala.concurrent.duration.Duration


object QueryCloudlet {
  def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy) =
    new QueryCloudlet(id, placement, opSchedStrategy)
}

class QueryCloudlet(val id: String, val placement: Placement, val opSchedStrategy: OpScheduleStrategy) {



  // a cloudlet should be  stateless. for each interval, a cloudlet will represent the execution of
  // all queries allocated to a VM.

  /**
    * Initialize all vertices from the cloudlet's placement.
    * @param startTime Execution start time (in milliseconds).
    */
  def init(startTime: Double): Unit = {
    placement.vertices.foreach(_.init(startTime))
  }

  /**
   * Enqueue into a vertex events received from another vertex that is currently running in
   * another placement.
   * @param receivedTime Time in which the events has been received (in milliseconds).
   * @param v Vertex that has received the events.
   * @param orig Origin of the received events.
   * @param events Number of events that has been received.
   * @return History containing the received event logged.
   */
  def enqueue(receivedTime: Double, v: InputVertex, orig: OutputVertex, events: Int): History[Entry] = {
    if (!placement.vertices.contains(v))
      throw new IllegalStateException("This cloudlet does not contain the target vertex")

    var history = History()
    v.enqueueIntoInput(orig, events)

    history = history.logReceived(id, receivedTime, v, orig, events)
    history
  }

  /**
   * Run the cloudlet for the specified number of instructions.
   * @param instructions Number of instructions that can be used in this simulation tick.
   * @param startTime The current simulation time (in milliseconds)..
   * @param capacity The total processor capacity (in MIPS) that is allocated to this cloudlet.
   * @return History containing all logged events.
   */
  def run(instructions: Double, startTime: Double, capacity: Double): History[Entry] = {
    var history = History()
    if (instructions > 0) {

      val availableInstructions = instructions
      val instructionsPerMs = (capacity * 1000)
      def totalMs(number: Double) = number / instructionsPerMs

      // generate the events before calling the scheduling strategy
      // in theory this enables more complex strategies that consider the number of
      // events to be consumed
      placement.producers foreach (_.generate())

      val verticesList = opSchedStrategy.allocate(availableInstructions, placement)
      var time = startTime

      verticesList.foreach { (elem) =>

        val v: Vertex = elem._1
        var processedEvents = 0.0
        if (v.isInstanceOf[InputVertex]) {

          val iv = v.asInstanceOf[InputVertex]

          // predecessors from all queries
          val predecessors = iv.queries.flatMap(_.predecessors(v))
          predecessors.foreach { (pred) =>
            val events = pred.outputQueues(iv)
            pred.dequeueFromOutput((iv, events))
            iv.enqueueIntoInput(pred, events)
          }
          processedEvents = iv.run(elem._2)

          if (iv.isBounded()) {
            predecessors.foreach { (pred) =>
              pred.setLimit(iv, iv.queueMaxSize - iv.inputQueues(pred))
            }
          }

        } else {
          processedEvents = v.run(elem._2)
        }


        history = history.logProcessed(id, time, v, processedEvents)

        // check if there are events to be sent to remote vertices
        if (v.isInstanceOf[OutputVertex]) {

          val ov = v.asInstanceOf[OutputVertex]
          val successors: Set[Vertex] = ov.queries.flatMap(_.successors(ov))
          val notInPlacement = successors -- placement.vertices


          notInPlacement.foreach { (dest) =>
            // log and remove from the output queue
            // the actual sending is not implemented here

            val sentMessages = Math.floor(ov.outputQueues(dest)).toInt
            if (sentMessages > 0) {
              history = history.logSent(id, time, v, dest, sentMessages)
              ov.dequeueFromOutput((dest, sentMessages))
            }
          }
        }

        time += totalMs(elem._2)
      }
    }

    history
  }

}