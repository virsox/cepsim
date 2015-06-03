package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.history.{Produced, SimEvent}

/** Operator companion object. */
object Operator {
  def apply(id: String, ipe: Double, queueMaxSize: Int = 0) =
    new Operator(id, ipe, queueMaxSize)//, selectivity: Double = 1.0) =
}

/**
 * Vertex that represents an operator. An operator is an InputVertex and an OuputVertex at the same time.
 * @param id Vertex identifier.
 * @param ipe Number of instructions needed to process one event.
 * @param queueMaxSize Maximum size of the input queues.
 */
class Operator(val id: String, val ipe: Double, val queueMaxSize: Int) extends Vertex
  with InputVertex
  with OutputVertex {


  /**
    * Executes the operator logic.
    * @param instructions Number of allocated instructions.
    * @param startTime Time at which the simulation of this vertex has started (in ms since the simulation start).
    * @param endTime Time at which the simulation of this vertex will end (in ms since the simulation start).
    * @return A list with a single Produced simulation event, or an empty list if no events has been produced.
    */
  override def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent] = {

    // number of processed events
    val fromInput = retrieveFromInput(instructions, maximumNumberOfEvents)

    val events = EventSet.addAll(fromInput.values)
    events.updateTimestamp(endTime)
    sendToAllOutputs(events)

    if (events.size == 0) List.empty
    else List(Produced(this, startTime, endTime, events))
  }

  /** The number of instructions needed to process all pending events. */

  /*
     TODO need to fix this condition - if the successor is in another cloudlet, the limit will not be updated
     until the other cloudlet is executed.
  */
  def instructionsNeeded: Double = totalInputEvents.min(maximumNumberOfEvents) * ipe


  def canEqual(other: Any): Boolean = other.isInstanceOf[Operator]

  override def equals(other: Any): Boolean = other match {
    case that: Operator =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = id.hashCode()
}