package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.gen.Generator

object EventProducer {
  def apply(id: String, ipe: Double, gen: Generator) = new EventProducer(id, ipe, gen)
}

class EventProducer(val id: String, val ipe: Double, val generator: Generator) extends Vertex
  with OutputVertex {

  var inputQueue = 0

  def run(instructions: Double): Int = {

    inputQueue += generator.generate()

    val maxOutput = (instructions / ipe) toInt
    val processed = Math.min(inputQueue, maxOutput)

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