package ca.uwo.eng.sel.cepsim.query


object EventConsumer {
  def apply(id: String, ipe: Double, queueMaxSize: Int = 0) = new EventConsumer(id, ipe, queueMaxSize)
}


class EventConsumer(val id: String, val ipe: Double, val queueMaxSize: Int) extends Vertex
  with InputVertex {

  var outputQueue = 0L
  var accumulated = 0.0

  def retrieveFromInput(instructions: Double): Map[Vertex, Double] = {

    // total number of input events
    val total = totalInputEvents

    // number of events that will be processed
    val events = total.min(instructions / ipe)

    // number of events processed from each queue
    // current implementation distribute processing according to the queue size
    var toProcess = inputQueues.map(elem =>
      (elem._1 -> (if (total == 0) 0.0 else (elem._2.toDouble / total) * events ))
    )

    // update the input queues
    dequeueFromInput(toProcess.toList:_*)

    // return the number of elements per input
    toProcess
  }

  override def run(instructions: Double, startTime: Double = 0.0): Double = {
    val processed = Vertex.sumOfValues(retrieveFromInput(instructions))

    var output = Math.floor(processed).toInt

    accumulated += processed - output
    if (Math.abs(accumulated - 1.0) < 0.01) {
      output += 1
      accumulated = Math.abs(accumulated - 1.0)
    }

    outputQueue += output
    output
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