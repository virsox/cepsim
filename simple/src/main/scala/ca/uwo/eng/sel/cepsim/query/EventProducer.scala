package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.gen.Generator

class EventProducer(val id: String, val ipe: Double, gen: Generator) extends Vertex
  with OutputVertex {

  var inputQueue = 0

  def run(instructions: Double): Unit = {

    inputQueue += gen.generate()

    val maxOutput = (instructions / ipe) toInt
    val processed = Math.min(inputQueue, maxOutput)

    inputQueue -= processed
    sendToAllOutputs(processed)
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