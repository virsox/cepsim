package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.gen.Generator

object EventProducer {
  def apply(id: String, ipe: Double, gen: Generator, limitProducer: Boolean = false) =
    new EventProducer(id, ipe, gen, limitProducer)
}

class EventProducer(val id: String, val ipe: Double, val generator: Generator, limitProducer: Boolean)
  extends Vertex with OutputVertex {

  var inputQueue = 0

  def generate() {
    if (limitProducer) inputQueue += generator.generate(maximumNumberOfEvents)
    else inputQueue += generator.generate()
  }

  def run(instructions: Double): Int = {

    val maxOutput = (instructions / ipe) toInt
    val processed = inputQueue.min(maxOutput).min(maximumNumberOfEvents)

    inputQueue -= processed
    sendToAllOutputs(processed)

    processed
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