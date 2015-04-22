package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.gen.Generator
import ca.uwo.eng.sel.cepsim.history.{Generated, Produced, SimEvent}
import ca.uwo.eng.sel.cepsim.metric.EventSet

/** EventProducer companion object. */
object EventProducer {
  def apply(id: String, ipe: Double, gen: Generator, limitProducer: Boolean = false) =
    new EventProducer(id, ipe, gen, limitProducer)
}

/**
  * Vertex that represents an event producer (source).
  * @param id Vertex identifier.
  * @param ipe Number of instructions needed to process one event.
  * @param generator Generator associated with this producer.
  * @param limitProducer Flag that indicates if the event generation is limited.
  */
class EventProducer(val id: String, val ipe: Double, val generator: Generator, limitProducer: Boolean)
  extends Vertex with OutputVertex {

  /** Event set of events generated but still not processed by the producer. */
  var inputEventSet = EventSet.empty()

  /** Number of events on the input event set. */
  def inputQueue = inputEventSet.size


  /**
    * Invokes the generator object in order to generate new events.
    * @param from Beginning of the period to be considered.
    * @param to End of the period.
    * @return A Generated simulation event.
    */
  def generate(from: Double, to: Double): SimEvent = {

    val interval = to - from
    val generated = (
      if (limitProducer) {
        val toGenerate = Math.floor((maximumNumberOfEvents - inputQueue).max(0)).toInt
        generator.generate(interval, toGenerate)
      } else generator.generate(interval)
    )

    val es = EventSet(generated, to, 0, Map(this -> generated))
    inputEventSet.add(es)

    Generated(this, from, to, es)
  }

  /**
    * Process the events generated and forward them to the output queues.
    * @param instructions Number of allocated instructions.
    * @param startTime Time at which the simulation of this vertex has started (in ms since the simulation start).
    * @param endTime Time at which the simulation of this vertex will end (in ms since the simulation start).
    * @return A list with a single Produced simulation event.
    */
  def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent] = {

    val maxOutput = (instructions / ipe)
    val processed = maxOutput.min(inputQueue).min(maximumNumberOfEvents)

    val es = inputEventSet.extract(processed)
    es.updateTimestamp(endTime)

    sendToAllOutputs(es)
    List(new Produced(this, startTime, endTime, es))
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[EventProducer]

  override def equals(other: Any): Boolean = other match {
    case that: EventProducer =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}