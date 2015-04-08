package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.history._
import ca.uwo.eng.sel.cepsim.metric._
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy

import scala.annotation.varargs
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration


object QueryCloudlet {
  def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy, iterations: Int = 1) =
    new QueryCloudlet(id, placement, opSchedStrategy, iterations)
}

class QueryCloudlet(val id: String, val placement: Placement, val opSchedStrategy: OpScheduleStrategy,
                    iterations: Int) {

  // --------------- Metric manipulation

  var calculatorsMap =  Map.empty[String, MetricCalculator]
  var calculators = Set.empty[MetricCalculator]

  def registerCalculator(calculator: MetricCalculator) = {
    calculator.ids.foreach((id) =>
      calculatorsMap = calculatorsMap updated (id, calculator)
    )
    calculators = calculators + calculator
  }

  def metric(id: String, v: Vertex) = calculatorsMap(id).consolidate(id, v)
  def metricList(id: String, v: Vertex) = calculatorsMap(id).results(id, v)

  // ---------------------------------------


  var lastExecution = 0.0

  /**
    * Initialize all vertices from the cloudlet's placement.
    * @param startTime Execution start time (in milliseconds).
    */
  @varargs def init(startTime: Double, calculators: MetricCalculator*): Unit = {
    calculators.foreach((calculator) => registerCalculator(calculator))
    placement.vertices.foreach(_.init(startTime))

    lastExecution = startTime
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
  def enqueue(receivedTime: Double, v: InputVertex, orig: OutputVertex, events: Int): History[SimEvent] = {
    if (!placement.vertices.contains(v))
      throw new IllegalStateException("This cloudlet does not contain the target vertex")

    var history = History()
    v.enqueueIntoInput(orig, events)

    // TODO do we need a received type of event?
    //history = history.logReceived(id, receivedTime, v, orig, events)
    history
  }

  /**
   * Run the cloudlet for the specified number of instructions.
   * @param instructions Number of instructions that can be used in this simulation tick.
   * @param startTime The current simulation time (in milliseconds)..
   * @param capacity The total processor capacity (in MIPS) that is allocated to this cloudlet.
   * @return History containing all logged events.
   */
  def run(instructions: Double, startTime: Double, capacity: Double): History[SimEvent] = {
    val history = History()
    var simEvents = ListBuffer.empty[SimEvent]

    if (instructions > 0) {

      // auxiliary function
      val instructionsPerMs = (capacity * 1000)
      def totalMs(number: Double) = number / instructionsPerMs

      val instructionsPerIteration = Math.floor(instructions / iterations).toLong
      var time = startTime

      (1 to iterations).foreach((i) => {

        val iterationSimEvents = ListBuffer.empty[SimEvent]

        // last iteration uses all remaining instructions
        val availableInstructions = if (i == iterations) instructions - ((i - 1) * instructionsPerIteration)
                                    else instructionsPerIteration


        // generate the events before calling the scheduling strategy
        // in theory this enables more complex strategies that consider the number of
        // events to be consumed
        placement.producers foreach ((prod) => {
          //val event = prod.generate(time, time + totalMs(availableInstructions))
          val event = prod.generate(lastExecution, time)
          //calculators.foreach(_.update(event))

          iterationSimEvents += event
        })
        lastExecution = time

        // Vertices execution

        val verticesList = opSchedStrategy.allocate(availableInstructions, placement)
        verticesList.foreach { (elem) =>

          val v: Vertex = elem._1
          val startTime = time
          val endTime = startTime + totalMs(elem._2)

          iterationSimEvents ++= v.run(elem._2, startTime, endTime)

          if (v.isInstanceOf[InputVertex]) {
            val iv = v.asInstanceOf[InputVertex]
            if (iv.isBounded()) {
              iv.predecessors.foreach { (pred) =>
                pred.setLimit(iv, iv.queueMaxSize - iv.inputQueues(pred))
              }
            }
          }

//          var processedEvents = 0.0
//          if (iterationSimEvents.length > 0) {
//            val lastSimEvent = iterationSimEvents(iterationSimEvents.length - 1)
//            processedEvents = lastSimEvent.quantity
//          }

//          if (processedEvents > 0)
//            history = history.logProcessed(id, startTime, v, processedEvents)
          //history.log(iterationSimEvents)


          // check if there are events to be sent to remote vertices
          if (v.isInstanceOf[OutputVertex]) {

            val ov = v.asInstanceOf[OutputVertex]

            val successors: Set[InputVertex] = ov.successors //queries.flatMap(_.successors(ov))
            val placementInputVertices = placement.vertices.collect{ case e: InputVertex => e}

            val inPlacement = successors.intersect(placementInputVertices)
            val notInPlacement = successors -- placementInputVertices


            notInPlacement.foreach { (dest) =>
              // log and remove from the output queue
              // the actual sending is not implemented here

              val sentMessages = Math.floor(ov.outputQueues(dest)).toInt
              if (sentMessages > 0) {
                // TODO think about how to model remote communication
//                history = history.logSent(id, startTime, v, dest, sentMessages)
                ov.dequeueFromOutput((dest, sentMessages))
              }
            }

            inPlacement.foreach { (dest) =>
              val events = ov.outputQueues(dest)
              ov.dequeueFromOutput((dest, events))
              dest.enqueueIntoInput(ov, events)

            }
          }

          time = endTime
        }

        iterationSimEvents.foreach((simEvent) =>
          calculators.foreach(_.update(simEvent))
        )
        history.log(iterationSimEvents)
        simEvents = simEvents ++ iterationSimEvents
      })
   }

    history
  }

}