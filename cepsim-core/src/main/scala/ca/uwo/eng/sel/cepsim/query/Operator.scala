package ca.uwo.eng.sel.cepsim.query

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

  /** *
    * Retrieve events from the input queues.
    * @param instructions Number of instructions that can be used.
    * @param maximumNumberOfEvents Maximum number of events that can be sent to the output queues.
    * @return Map from the predecessors to the number of events retrieved.
    */
  def retrieveFromInput(instructions: Double, maximumNumberOfEvents: Double = Double.MaxValue): Map[Vertex, Double] = {

    // total number of input events
    val total = totalInputEvents

    // number of events that can be processed
    val events = total.min(instructions / ipe).min(maximumNumberOfEvents)

    // number of events processed from each queue
    // current implementation distribute processing according to the queue size
    val toProcess = inputQueues.map(elem =>
      (elem._1 -> (if (total == 0) 0.0 else (elem._2.toDouble / total) * events))
    )

    // update the input queues
    dequeueFromInput(toProcess.toList:_*)

    // return the number of elements per input
    toProcess
  }

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
    val events = Vertex.sumOfValues(fromInput)

    sendToAllOutputs(events)

    if (events == 0) List.empty
    else List(Produced(this, startTime, endTime, events, fromInput))
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[Operator]

  override def equals(other: Any): Boolean = other match {
    case that: Operator =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}