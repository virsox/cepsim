package ca.uwo.eng.sel.cepsim.query


object Operator {
  def apply(id: String, ipe: Double) = new Operator(id, ipe)//, selectivity: Double = 1.0) =
}

class Operator(val id: String, val ipe: Double) extends Vertex
  with InputVertex
  with OutputVertex {

  var accumulator: Double = 0


  override def run(instructions: Double): Int = {

    // number of processed events
    val events = totalFromMap(retrieveFromInput(instructions))

    sendToAllOutputs(events)
    events
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