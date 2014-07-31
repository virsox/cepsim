package ca.uwo.eng.sel.cepsim.query


object EventConsumer {
  def apply(id: String, ipe: Double) = new EventConsumer(id, ipe)
}


class EventConsumer(val id: String, val ipe: Double) extends Vertex
  with InputVertex {

  var outputQueue = 0

  override def run(instructions: Double): Unit = {
    val processed = totalFromMap(retrieveFromInput(instructions))
    outputQueue += processed
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