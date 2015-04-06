package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.{WindowAccumulated, WindowProduced, SimEvent}

import scala.concurrent.duration._


/** WindowedOperator companion object. */
object WindowedOperator {

  def apply(id: String, ipe: Double, size: Double, advance: Double, function: (Map[Vertex, Double]) => Double) =
    new WindowedOperator(id, ipe, size milliseconds, advance milliseconds, function, 1000)

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
  * @param queueMaxSize Maximum size of the input queues, if limited.
  */
class WindowedOperator(id: String, ipe: Double, val size: Duration, val advance: Duration,
                      function: (Map[Vertex, Double]) => Double, queueMaxSize: Int)
  extends Operator(id, ipe, queueMaxSize) {

  /** Start time. */
  var start = 0.0

  /** Next timestamp at which the operator should emit events. */
  var processAt: Double = 0.0

  /** Current slot number. */
  var currentIndex = 0

  /** Number of slots. */
  val slots = (size.toUnit(MILLISECONDS) / advance.toUnit(MILLISECONDS)).toInt

  /** Slot vector. Each position contains a map counting the number of events received from each predecessor. */
  var accumulated: Vector[Map[Vertex, Double]] =  Vector.fill(slots)(Map.empty withDefaultValue(0.0))

  /** Slot on which events have been accumulated on the last operator execution. */
  var accumulatedSlot = 0

  /**
    * Initializes the operator.
    * @param startTime Initialization time (in milliseconds since the simulation start).
    * @param simInterval Simulation tick duration (in milliseconds).
    */
  override def init(startTime: Double = 0.0, simInterval: Double = 10.0): Unit = {
    start = startTime
    processAt = start + advance.toUnit(MILLISECONDS)
  }

  /**
    * Executes the operator.
    * @param instructions Number of allocated instructions.
    * @param startTime
    * @return Number of processed events.
    */
  override def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent] = {
    var events = List.empty[SimEvent]
    var retrievedEvents = Map.empty[Vertex, Double]

    // this loop advances the processAt attribute to the next timestamp at which the operator
    // emit events and executes the aggregation function on all windows that have passed
    // it is not expected that more than one window has passed though - first, because
    // the windows are usually large (measured in minutes). Second, the default scheduling
    // strategies will always try to execute operators which have events on their input queues.
    while (startTime >= processAt) {

      // a window has passed
      val total = totalAccumulated()
      if (Vertex.sumOfValues(total) > 0) {
        val outputTotal = function(total)
        sendToAllOutputs(outputTotal)

        events = events :+ WindowProduced(this, startTime, endTime, outputTotal, currentIndex)
      }

      processAt = processAt + advance.toUnit(MILLISECONDS)
      currentIndex = (currentIndex + 1) % slots
      reset(currentIndex)

    }


    // In case more than one window has passed and the previous loop is executed more than once,
    // the accumulation still happens at the current slot only. This is not 100% correct: the input
    // queue could have events that should be accumulated on previous slots. Nevertheless, the implementation
    // of this algorithm would require tracking of event timestamps in the queue. Conversely, our approach has
    // been chosen because it simplifies the simulator implementation and the error it introduces is
    // small: latency and throughput calculation are not affected, but the number of output events might
    // be wrong because the number of events on each slot is not correct. However, note that the error
    // occurs only in case the aggregation function is not a constant.
    //

    // accumulate events in current slot
    retrievedEvents = accumulate(instructions)
    if (Vertex.sumOfValues(retrievedEvents) > 0)
      events = events :+ WindowAccumulated(this, startTime, endTime, currentIndex, retrievedEvents)

    events
  }

  /**
    * Accumulate events coming from the input queues.
    * @param instructions Number of instructions to be executed.
    * @return Map from the predecessors to the number of events processed from each one of them.
    */
  private def accumulate(instructions: Double): Map[Vertex, Double] = {
    // retrieve events from input
    val retrievedEvents = retrieveFromInput(instructions, maximumNumberOfEvents)
    retrievedEvents.foreach((elem) => {
      accumulated = accumulated updated (currentIndex,
        accumulated(currentIndex) updated (elem._1, accumulated(currentIndex)(elem._1) + elem._2))
    })
    accumulatedSlot = currentIndex
    retrievedEvents
  }

  /**
    * Reset a slot.
    * @param index Slot number.
    */
  private def reset(index: Int): Unit = {
    accumulated = accumulated updated (index, Map.empty withDefaultValue(0.0))
  }

  /**
    * Returns the total number of accumulated events in the window.
    * @return Map from vertex predecessors to total number of accumulated events from them.
    */
  private def totalAccumulated(): Map[Vertex, Double] = {
    accumulated.foldLeft(Map.empty[Vertex, Double])((acc, elem) => {
      elem.map((e) => (e._1, acc.getOrElse(e._1, 0.0) + e._2)) ++ acc.filterKeys(!elem.contains(_))
    })
  }

}
