package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.metric.History
import ca.uwo.eng.sel.cepsim.metric.History.Entry
import ca.uwo.eng.sel.cepsim.metrics._
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy

import scala.annotation.varargs
import scala.concurrent.duration.Duration


object QueryCloudlet {
  def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy) =
    new QueryCloudlet(id, placement, opSchedStrategy)
}

class QueryCloudlet(val id: String, val placement: Placement, val opSchedStrategy: OpScheduleStrategy) {

  // --------------- Metric manipulation

  var calculators =  Map.empty[String, MetricCalculator]
  //List(LatencyMetric.calculator(placement)) // List.empty[MetricCalculator]//.calculator(placement))

  def registerCalculator(id: String, calculator: MetricCalculator) =
    calculators = calculators updated (id, calculator)

  def metric(id: String) = calculators(id).consolidate
  def metricList(id: String) = calculators(id).results

  // ---------------------------------------





  // a cloudlet should be  stateless. for each interval, a cloudlet will represent the execution of
  // all queries allocated to a VM.

  /**
    * Initialize all vertices from the cloudlet's placement.
    * @param startTime Execution start time (in milliseconds).
    */
  @varargs def init(startTime: Double, calculators: MetricCalculator*): Unit = {
    calculators.foreach((calculator) => registerCalculator(calculator.id, calculator))
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
      placement.producers foreach ((prod) => {
        val generated = prod.generate()
        val event = Produced(prod, generated, startTime)
        calculators.values.foreach(_.update(event))
      })

//      case class Produced (val v: Vertex, val quantity: Double, val from: Double, val at: Double) extends Event
//      case class Processed(val v: Vertex, val quantity: Double, val at: Double, val queues: Map[Vertex, Double] = Map.empty) extends Event
//      case class Consumed (val v: Vertex, val quantity: Double, val at: Double, val queues: Map[Vertex, Double]) extends Event
//
      val verticesList = opSchedStrategy.allocate(availableInstructions, placement)
      var time = startTime

      verticesList.foreach { (elem) =>

        val v: Vertex = elem._1
        var processedEvents = 0.0
        var processedQueues = Map.empty[Vertex, Double]

        if (v.isInstanceOf[InputVertex]) {

          val iv = v.asInstanceOf[InputVertex]

          // predecessors from all queries
          val predecessors = iv.queries.flatMap(_.predecessors(v))

          // ------------------ queue status before running vertex
          var before = Map.empty[Vertex, Double]
          // ------------------------------------------------------------------------------

          predecessors.foreach { (pred) =>
            val events = pred.outputQueues(iv)
            pred.dequeueFromOutput((iv, events))
            iv.enqueueIntoInput(pred, events)

            if (!calculators.isEmpty)
              before = before updated (pred, iv.inputQueues(pred))
          }
          processedEvents = iv.run(elem._2, time)

          // ------------------ queue status after running vertex
          before.foreach((entry) => {
            processedQueues = processedQueues updated (entry._1, entry._2 - iv.inputQueues(entry._1))
          })
          // ------------------------------------------------------------------------------


          if (iv.isBounded()) {
            predecessors.foreach { (pred) =>
              pred.setLimit(iv, iv.queueMaxSize - iv.inputQueues(pred))
            }
          }

          // ------------------ build event
         // event = Processed(v, processedEvents, )


        } else {
          processedEvents = v.run(elem._2, time)
        }

        if (processedEvents > 0)
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

        // ---------------- Update metrics
        var event: Event = null;
        if (v.isInstanceOf[EventProducer]) {
          event = Processed(v, processedEvents, time)
        } else if (v.isInstanceOf[EventConsumer]) {
          event = Consumed(v, processedEvents, time, processedQueues)
        } else {
          event = Processed(v, processedEvents, time, processedQueues)
        }
        calculators.values.foreach(_.update(event))
        // ------------------------------------------------------------------------------

      }
    }

    history
  }

}