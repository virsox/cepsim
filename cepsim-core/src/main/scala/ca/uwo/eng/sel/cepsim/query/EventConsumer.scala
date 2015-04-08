package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.{Consumed, SimEvent}

/** EventConsumer companion object. */
object EventConsumer {
  def apply(id: String, ipe: Double, queueMaxSize: Int = 0) = new EventConsumer(id, ipe, queueMaxSize)
}

/**
  * Vertex that represents an event consumer (sink).
  * @param id Vertex identifier.
  * @param ipe Number of instructions needed to process one event.
  * @param queueMaxSize Maximum size of the input queues.
  */
class EventConsumer(val id: String, val ipe: Double, val queueMaxSize: Int) extends Vertex
  with InputVertex {

  /** Total number of consumed events. */
  var outputQueue = 0L

  /**
    * Because event consumers only consumes full events (does not consume partial events), this variable
    * stores the decimal parts that haven't been consumed.
    */
  var accumulated = 0.0

  /**
    * Retrieve events from the input queues.
    * @param instructions Number of instructions that can be used.
    * @return Map from the predecessors to the number of events retrieved.
    */
  def retrieveFromInput(instructions: Double): Map[Vertex, Double] = {

    // total number of input events
    val total = totalInputEvents

    // number of events that will be processed
    val events = total.min(instructions / ipe)

    // number of events processed from each queue
    // current implementation distribute processing according to the queue size
    var toProcess = inputQueues.map(elem =>
      (elem._1 -> (if (total == 0) 0.0 else (elem._2.toDouble / total) * events ))
    )

    // update the input queues
    dequeueFromInput(toProcess.toList:_*)

    // return the number of elements per input
    toProcess
  }

  /**
    * Consumes events from the input queues.
    * @param instructions Number of allocated instructions.
    * @param startTime Time at which the simulation of this vertex has started (in ms since the simulation start).
    * @param endTime Time at which the simulation of this vertex will end (in ms since the simulation start).
    * @return A List with a single Consumed simulation event, or an empty list if no events have been consumed.
    */
  override def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent] = {
    val fromInput = retrieveFromInput(instructions)
    val processed = Vertex.sumOfValues(fromInput)

    var output = Math.floor(processed).toInt

    // accumulates the decimal part
    accumulated += processed - output

    // if it larger than one, then it is possible to emit more events
    while (accumulated > 1.0) {
      output += 1
      accumulated -= 1.0
    }

    // consider this is true if there were double rounding errors
    if (Math.abs(accumulated - 1.0) < 0.01) {
      output += 1
      accumulated = 0.0
    }
    outputQueue += output

    if ((output == 0) && (processed == 0)) List()
    else List(Consumed(this, startTime, endTime, output, fromInput))
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[EventConsumer]

  override def equals(other: Any): Boolean = other match {
    case that: EventConsumer =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}