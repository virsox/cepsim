package ca.uwo.eng.sel.cepsim.query


object Operator {
  def apply(id: String, ipe: Double, queueMaxSize: Int = 0) =
    new Operator(id, ipe, queueMaxSize)//, selectivity: Double = 1.0) =
}

class Operator(val id: String, val ipe: Double, val queueMaxSize: Int) extends Vertex
  with InputVertex
  with OutputVertex {

  var accumulator: Double = 0

  def retrieveFromInput(instructions: Double, maximumNumberOfEvents: Double= Double.MaxValue): Map[Vertex, Double] = {

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

  override def run(instructions: Double): Double = {

    // number of processed events
    val events = sumOfValues(retrieveFromInput(instructions, maximumNumberOfEvents))

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