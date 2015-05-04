package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.{Produced, WindowAccumulated, SimEvent}
import ca.uwo.eng.sel.cepsim.metric.EventSet

import scala.collection.immutable.Queue
import scala.concurrent.duration._


/** WindowedOperator companion object. */
object WindowedOperator {

  def apply(id: String, ipe: Double, size: Double, advance: Double, function: (Map[Vertex, Double]) => Double) =
    new WindowedOperator(id, ipe, size milliseconds, advance milliseconds, function, true, 1024)

  def apply(id: String, ipe: Double, size: Double, advance: Double, function: (Map[Vertex, Double]) => Double,
            queueMaxSize: Int) =
    new WindowedOperator(id, ipe, size milliseconds, advance milliseconds, function, true, queueMaxSize)

  def apply(id: String, ipe: Double, size: Double, advance: Double, function: (Map[Vertex, Double]) => Double,
            limitOutput: Boolean, queueMaxSize: Int) =
    new WindowedOperator(id, ipe, size milliseconds, advance milliseconds, function, limitOutput, queueMaxSize)


  /** Returns an identify function to be used with WindowedOperators. */
  def identity(): (Map[Vertex, Double]) => Double = ((x) => Vertex.sumOfValues(x))

  /**
    * Returns a constant function to be used with WindowedOperators.
    * @param c Constant number to be returned.
    */
  def constant(c: Double): (Map[Vertex, Double]) => Double = ((x) => c)

}

/**
  * Query operator based on windows. These operators process windows of events and combine them using an
  * aggregation function. Typical examples are aggregation operators that count events or calculate the average
  * value of attributes.
  *
  * @param id           Operator identifier.
  * @param ipe          Number of CPU instructions needed to process a single event.
  * @param size         Size of the window. It specifies the period of time from which the events are taken.
  * @param advance      How the window advance. It defines how the window slides when the previous window closes.
  * @param function     The aggregation function. It receives a map from predecessors to the number of events received
  *                    from it in the last window, and it returns the number of events that must be output by
  *                    the operator.
  * @param limitOutput  Flag that indicate if the operator should limit its output when generating tuples.
  * @param queueMaxSize Maximum size of the input queues, if limited.
  */
class WindowedOperator(id: String, ipe: Double, val size: Duration, val advance: Duration,
                      function: (Map[Vertex, Double]) => Double, limitOutput: Boolean, queueMaxSize: Int)
  extends Operator(id, ipe, queueMaxSize) {

  /** Start time. */
  var start = 0.0

  /** Next timestamp at which the operator should emit events. */
  var processAt: Double = 0.0

  /** Current slot number. */
  var currentIndex = 0

  /** Number of slots. */
  val slots = (size.toUnit(MILLISECONDS) / advance.toUnit(MILLISECONDS)).toInt

  /** Slot vector. Each position contains a map counting the number of event sets received from each predecessor. */
  var accumulated: Vector[Map[Vertex, EventSet]] =  Vector.fill(slots)(Map.empty)

  /** Slot on which events have been accumulated on the last operator execution. */
  var accumulatedSlot = 0

  /** Events to be sent to successors. They are enqueued here when the successor buffers are full. */
  var toBeSent = Queue[EventSet]()

  /**
    * Initializes the operator.
    * @param startTime Initialization time (in milliseconds since the simulation start).
    * @param simInterval Simulation tick duration (in milliseconds).
    */
  override def init(startTime: Double = 0.0, simInterval: Double = 10.0): Unit = {
    start = startTime
    processAt = start + advance.toUnit(MILLISECONDS)
  }

  /** Indicates if the vertex has pending events and needs to be allocated. */
  override def needsAllocation: Boolean = !toBeSent.isEmpty || instructionsNeeded > 0.0

  /**
    * Add a new input queue to the windowed operator. Overrides the InputVertex definition because
    * it also needs to initialize the accumulated map.
    * @param v New predecessor vertex.
    */
  override def addInputQueue(v: Vertex) = {
    super.addInputQueue(v)
    accumulated = accumulated.map((elem) => elem.updated(v, EventSet.empty()))
  }

  /**
    * Executes the operator.
    * @param instructions Number of allocated instructions.
    * @param startTime
    * @return Sequence of simulation events produced by the operator.
    */
  override def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent] = {
    var events = List.empty[SimEvent]

    // first thing to do is check if there are pending events to be sent to the successors
    var availableSpace = maximumNumberOfEvents
    while ((!toBeSent.isEmpty) && (availableSpace > 0)) {

      val(dequeuedEs, newQueue) = toBeSent.dequeue

      // check the number of events that can be sent
      var eventsNo = dequeuedEs.size min availableSpace
      availableSpace -= eventsNo

      val eventSet = dequeuedEs.extract(eventsNo)
      eventSet.updateTimestamp(endTime)
      sendToAllOutputs(eventSet)
      events = events :+ Produced(this, startTime, endTime, eventSet)

      // enqueue the event set again if not all events have been sent
      toBeSent = if (dequeuedEs.size == 0) newQueue else dequeuedEs +: newQueue
    }


    // this loop advances the processAt attribute to the next timestamp at which the operator
    // emit events and executes the aggregation function on all windows that have passed
    // it is not expected that more than one window has passed though - first, because
    // the windows are usually large (measured in minutes). Second, the default scheduling
    // strategy will always try to execute operators which have events on their input queues.
    while (startTime >= processAt) {

      // a window has passed
      val total = totalAccumulated()
      if (EventSet.totalSize(total.values) > 0) {

        val functionTotal = function(total.map((e) => e._1 -> e._2.size))

        // the size of the output eventSet is the result of the function execution
        val eventSum = EventSet.addAll(total.values)
        eventSum.size = functionTotal

        // the totals map refers only to the events on the current slot (previous totals had been
        // considered on previous windows)
        eventSum.totals = accumulated(currentIndex).foldLeft(EventSet.empty)((acc, elem) => {
          acc.add(elem._2)
          acc
        }).totals

        // check the successor queues for elements when the window closes. If there is any,
        // then some of the generated tuples are discarded. The rationale for this process is that the
        // successor could not process all elements between windows - which means that if all we send
        // events again, the latency will keep increasing
        if (limitOutput) {
          eventSum.extract(maxOutputQueueSize())
        }


        val output = functionTotal min availableSpace
        if (output > 0) {

          // if it is here, then there is space left on the output buffers
          val(outputEs, remainingEs) = eventSum.split(output / functionTotal)
          outputEs.updateTimestamp(endTime)
          sendToAllOutputs(outputEs)

          events = events :+ Produced(this, startTime, endTime, outputEs)

          // if the output is not entirely sent, the remaining part is enqueued again
          if (output < functionTotal)
            toBeSent = toBeSent enqueue remainingEs

        } else {
          toBeSent = toBeSent enqueue eventSum
        }

        availableSpace -= output
      }

      processAt = processAt + advance.toUnit(MILLISECONDS)
      currentIndex = (currentIndex + 1) % slots
      reset(currentIndex)

    }


    // In case more than one window has passed and the previous loop is executed more than once
    // the accumulation still happens at the current slot only. This is not 100% correct: the input
    // queue could have events that should be accumulated on previous slots. Nevertheless, the implementation
    // of this algorithm would require tracking of event timestamps in the queue. Conversely, our approach has
    // been chosen because it simplifies the simulator implementation and the error it introduces is
    // small: latency and throughput calculation are not affected, but the number of output events might
    // be wrong because the number of events on each slot is not correct. However, note that the error
    // occurs only in case the aggregation function is not a constant.
    //

    // accumulate events in current slot
    val retrievedEvents = EventSet.addAll(accumulate(instructions).values)
    if (retrievedEvents.size > 0)
      events = events :+ WindowAccumulated(this, startTime, endTime, currentIndex, retrievedEvents)

    events
  }

  /**
    * Accumulate events coming from the input queues.
    * @param instructions Number of instructions to be executed.
    * @return Map from predecessors to an event set encapsulating events coming from a specific predecessor.
    */
   private def accumulate(instructions: Double): Map[Vertex, EventSet] = {
    // retrieve events from input - it doesn't need to respect the maximum number of events
    // because there's no direct relation between the number of input events consumed and the
    // number of output events generated
    val retrievedEvents = retrieveFromInput(instructions)
    retrievedEvents.foreach( (elem) => { accumulated(currentIndex)(elem._1).add(elem._2) })
    accumulatedSlot = currentIndex
    retrievedEvents
  }


  /**
    * Calculates the maximum number of elements (normalized by selectivity) in the successors queues.
    * @return Maximum number of elements.
    */
  private def maxOutputQueueSize(): Double =
    successors.map((v) => v.inputQueues(this) / selectivities(v)).
               foldLeft(Double.MinValue)((acc, number) => acc.max(number))


  /**
    * Reset a slot.
    * @param index Slot number.
    */
  private def reset(index: Int): Unit = {
    accumulated(index).foreach(_._2.reset())
  }

  /**
    * Returns the total number of accumulated events in the window.
    * @return Map from vertex predecessors to event sets encapsulating events accumulated from them.
    */
  private def totalAccumulated(): Map[Vertex, EventSet] = {

    val initMap = predecessors.map(_ -> EventSet.empty).toMap[Vertex, EventSet]
    accumulated.foldLeft(initMap)((acc, elem) => {
      // all elements of the slot
      elem.foreach((e) => acc(e._1).add(e._2))
      acc
    })
  }

}
