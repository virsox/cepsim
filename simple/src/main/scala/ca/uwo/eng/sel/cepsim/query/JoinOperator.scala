package ca.uwo.eng.sel.cepsim.query

object JoinOperator {
  def apply(id: String, ipe: Double, reduction: Double, queueMaxSize: Int = 0) =
    new JoinOperator(id, ipe, reduction, queueMaxSize)

}

class JoinOperator(id: String, ipe: Double, val reduction: Double, queueMaxSize: Int)
    extends Operator(id, ipe, queueMaxSize)
    with InputVertex
    with OutputVertex {


  /**
    * Estimate the number of events that should be consumed on each input queue in order to
    * respect the maximum number of events that this operator can generate.
    *
    * WARNING: The estimation breaks the encapsulation of the retrieveFromInput method because it
    * knows that the number of events processed from each queue is proportional to the queue size.
    *
    * @return Map containing a predecessor and the max number of events that should be consumed from
    *         that predecessor.
    */
  private[query] def estimation(): Map[Vertex, Int] = {

    // get the first vertex
    val first = inputQueues.keysIterator.next()

    // calculate the relation (b.queueSize / first.queueSize) for all vertices
    val proportion = inputQueues.map((elem) => (elem._1, elem._2.toDouble / inputQueues(first)))

    // multiply all relations, and multiply by the reduction factor
    val denominator = proportion.foldLeft(1.0)((accum, elem) => accum * elem._2) * reduction

    val numerator = maximumNumberOfEvents

    // value is how many events the first vertex needs to process
    val value = Math.pow(numerator / denominator, 1.0 / inputQueues.size)

    // then, we calculate for each vertex
    proportion.map((elem) => (elem._1, Math.floor(value * elem._2).toInt))
  }

  override def run(instructions: Double): Int = {

    val events = retrieveFromInput(instructions, sumOfValues(estimation()))

    // calculate the cartesian product among all input events
    val total = events.foldLeft(1)((accum, elem) => accum * elem._2)

    // the reduction parameter represents how much the join condition reduces
    // the number of joined elements
    sendToAllOutputs(Math.floor(total * reduction).toInt)
    sumOfValues(events)
  }

}
