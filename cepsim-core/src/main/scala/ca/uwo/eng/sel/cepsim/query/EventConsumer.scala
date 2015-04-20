package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.{Consumed, SimEvent}
import ca.uwo.eng.sel.cepsim.metric.EventSet

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

  /** EventSet containing all events consumed in the simulation so far. */
  val outputEventSet = EventSet.empty

  /** Total number of consumed events. */
  def outputQueue: Int = Math.floor(outputEventSet.size).toInt

  /**
    * Because event consumers only consumes full events (does not consume partial events), this set
    * accumulates the decimal parts that haven't been consumed.
    */
  var accumulated = EventSet.empty()

  /**
    * Consumes events from the input queues.
    * @param instructions Number of allocated instructions.
    * @param startTime Time at which the simulation of this vertex has started (in ms since the simulation start).
    * @param endTime Time at which the simulation of this vertex will end (in ms since the simulation start).
    * @return A List with a single Consumed simulation event, or an empty list if no events have been consumed.
    */
  override def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent] = {
    val fromInput = retrieveFromInput(instructions)

    val processed = EventSet.addAll(fromInput.values)
    processed.updateTimestamp(endTime)

    val diff = processed.size -  Math.floor(processed.size)

    // accumulates the decimal part
    if (diff > 0) {
      accumulated.add(processed.extract(diff))
    }

    // if it larger than one, then it is possible to emit more events
    while (accumulated.size > 1.0) {
      val es = accumulated.extract(1.0)
      es.updateTimestamp(endTime)
      processed.add(es)
    }

    // consider this is true if there were double rounding errors
    if (Math.abs(accumulated.size - 1.0) < 0.001) {
      accumulated.size = 1.0
      accumulated.updateTimestamp(endTime)
      processed.add(accumulated)
      accumulated.reset()
    }

    outputEventSet.add(processed)
    if (processed.size == 0) List()
    else List(Consumed(this, startTime, endTime, processed))
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