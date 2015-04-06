package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.gen.Generator
import ca.uwo.eng.sel.cepsim.history.{Produced, Generated, SimEvent}

object EventProducer {
  def apply(id: String, ipe: Double, gen: Generator, limitProducer: Boolean = false) =
    new EventProducer(id, ipe, gen, limitProducer)
}

class EventProducer(val id: String, val ipe: Double, val generator: Generator, limitProducer: Boolean)
  extends Vertex with OutputVertex {

  var inputQueue = 0.0

  def generate(from: Double, to: Double): SimEvent = {

    val interval = to - from
    val generated = (if (limitProducer) generator.generate(interval, Math.floor(maximumNumberOfEvents).toInt)
                     else generator.generate(interval))

    inputQueue += generated
    Generated(this, from, to, generated)
  }

  def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent] = {

    val maxOutput = (instructions / ipe)

    val processed = maxOutput.min(inputQueue).min(maximumNumberOfEvents)

    inputQueue -= processed
    sendToAllOutputs(processed)

    List(Produced(this, startTime, endTime, processed))
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