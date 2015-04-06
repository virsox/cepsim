package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.{Consumed, SimEvent}


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

  override def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent] = {
    val fromInput = retrieveFromInput(instructions)
    val processed = Vertex.sumOfValues(fromInput)

    var output = Math.floor(processed).toInt

    accumulated += processed - output
    while (accumulated > 1.0) {
      output += 1
      accumulated -= 1.0
    }

    // consider this is true if there were double rounding errors
    if (Math.abs(accumulated - 1.0) < 0.01) {
      output += 1
      accumulated = 0.0
    }
    outputQueue += output

    if ((output == 0) && (processed == 0)) List()
    else List(Consumed(this, startTime, endTime, output, fromInput))
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