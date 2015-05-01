package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.history._
import ca.uwo.eng.sel.cepsim.metric._
import ca.uwo.eng.sel.cepsim.network.NetworkInterface
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.{EnqueueAction, Action, ExecuteAction, OpScheduleStrategy}

import scala.annotation.varargs
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer


/** QueryCloudlet companion object */
object QueryCloudlet {

  def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy, iterations: Int = 1) =
    new QueryCloudlet(id, placement, opSchedStrategy, iterations, null)

  def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy, iterations: Int,
            networkInterface: NetworkInterface) =
    new QueryCloudlet(id, placement, opSchedStrategy, iterations, networkInterface)


  @varargs def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy, iterations: Int,
             calculator: MetricCalculator*) =
    new QueryCloudlet(id, placement, opSchedStrategy, iterations, null, calculator:_*)

  @varargs def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy, iterations: Int,
            networkInterface: NetworkInterface, calculator: MetricCalculator*) =
    new QueryCloudlet(id, placement, opSchedStrategy, iterations, networkInterface, calculator:_*)


//  def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy,
//            iterations: Int = 1, networkInterface: NetworkInterface = null,
//            metricCalculators: Set[MetricCalculator] = Set.empty) =
//    new QueryCloudlet(id, placement, opSchedStrategy, iterations, networkInterface, metricCalculators.toSeq:_*)
//
//  def apply(id: String, placement: Placement, opSchedStrategy: OpScheduleStrategy,
//            iterations: Int, networkInterface: NetworkInterface = null) =
//    new QueryCloudlet(id, placement, opSchedStrategy, iterations, null)
}

@varargs class QueryCloudlet(val id: String, val placement: Placement, opSchedStrategy: OpScheduleStrategy,
                    val iterations: Int, var networkInterface: NetworkInterface,
                    metricCalculators: MetricCalculator*) {

  // --------------- Metric manipulation

  private var calculatorsMap =  Map.empty[String, MetricCalculator]
  private var calculators = Set.empty[MetricCalculator]

  private def isRegistered(calculator: MetricCalculator): Boolean = {
    calculators.exists(_.getClass() == calculator.getClass())
  }

  def registerCalculator(calculator: MetricCalculator) = {
    if (!isRegistered(calculator)) {
      calculator.ids.foreach((id) =>
        calculatorsMap = calculatorsMap updated (id, calculator)
      )
      calculators = calculators + calculator
    }
  }

  // register all metrics
  metricCalculators.foreach(registerCalculator(_))


  def metric(id: String, v: Vertex) = calculatorsMap(id).consolidate(id, v)
  def metricList(id: String, v: Vertex) = calculatorsMap(id).results(id, v)

  // ---------------------------------------


  // --------------- Network interface




  var lastExecution = 0.0
  var pendingActions = TreeSet.empty[Action]


  /**
    * Initialize all vertices from the cloudlet's placement.
    * @param startTime Execution start time (in milliseconds).
    */
  def init(startTime: Double): Unit = {
    placement.vertices.foreach(_.init(startTime))

    lastExecution = startTime
  }

  /**
   * Enqueue into a vertex events received from another vertex that is currently running in
   * another placement.
   * @param receivedTime Time in which the events have been received (in milliseconds).
   * @param orig Origin of the received events.
   * @param v Vertex that has received the events.
   * @param es EventSet to be enqueued.
   * @return History containing the received event logged.
   */
  def enqueue(receivedTime: Double, orig: OutputVertex, v: InputVertex, es: EventSet): Unit = {
    if (!placement.vertices.contains(v))
      throw new IllegalStateException("This cloudlet does not contain the target vertex")

    pendingActions += EnqueueAction(v, orig, receivedTime, es)
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

      val instructionsPerIteration = Math.floor(instructions / iterations).toLong
      var iterationStartTime = startTime

      (1 to iterations).foreach((i) => {

        val iterationSimEvents = ListBuffer.empty[SimEvent]

        // last iteration uses all remaining instructions
        val availableInstructions = if (i == iterations) instructions - ((i - 1) * instructionsPerIteration)
                                    else instructionsPerIteration


        // generate the events before calling the scheduling strategy
        // in theory this enables more complex strategies that consider the number of
        // events to be consumed
        placement.producers foreach ((prod) => {
          val event = prod.generate(lastExecution, iterationStartTime)
          event match {
            case Some(ev) => iterationSimEvents += ev
            case None =>
          }
        })
        lastExecution = iterationStartTime

        // Vertices execution
        val verticesList = opSchedStrategy.allocate(availableInstructions, iterationStartTime, capacity, placement, pendingActions)
        verticesList.foreach { (elem) =>
          elem match {
            case executeAction: ExecuteAction => iterationSimEvents ++= execute(executeAction)
            case enqueueAction: EnqueueAction => execute(enqueueAction)
          }
        }
        iterationStartTime += (availableInstructions / (capacity * 1000))


        iterationSimEvents.foreach((simEvent) =>
          calculators.foreach(_.update(simEvent))
        )
        history.log(iterationSimEvents)
        simEvents = simEvents ++ iterationSimEvents
      })
   }

    history
  }

  private def execute(action: EnqueueAction) = {
    action.v.enqueueIntoInput(action.fromVertex, action.es)
  }

  private def execute(action: ExecuteAction): Seq[SimEvent] = {
    val v: Vertex = action.v
    val startTime = action.from
    val endTime = action.to//startTime + totalMs(elem._2)

    val simEvents = v.run(action.asInstanceOf[ExecuteAction].instructions, startTime, endTime)

    if (v.isInstanceOf[InputVertex]) {
      val iv = v.asInstanceOf[InputVertex]
      if (iv.isBounded()) {
        iv.predecessors.foreach { (pred) =>
          pred.setLimit(iv, iv.queueMaxSize - iv.inputQueues(pred))
        }
      }
    }

    // check if there are events to be sent to remote vertices
    if (v.isInstanceOf[OutputVertex]) {

      val ov = v.asInstanceOf[OutputVertex]

      val successors: Set[InputVertex] = ov.successors
      val placementInputVertices = placement.vertices.collect{ case e: InputVertex => e}

      val inPlacement = successors.intersect(placementInputVertices)
      val notInPlacement = successors -- placementInputVertices


      notInPlacement.foreach { (dest) =>
        val events = ov.dequeueFromOutput(dest, ov.outputQueues(dest))
        networkInterface.sendMessage(endTime, ov, dest, events)
      }

      inPlacement.foreach { (dest) =>
        val events = ov.outputQueues(dest)
        dest.enqueueIntoInput(ov, ov.dequeueFromOutput(dest, events))
      }
    }

    simEvents
  }

}