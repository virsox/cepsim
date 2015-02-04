package ca.uwo.eng.sel.cepsim.query

import scala.concurrent.duration._

object JoinOperator {
  def apply(id: String, ipe: Double, reduction: Double, window: Duration, queueMaxSize: Int = 0) =
    new JoinOperator(id, ipe, reduction, window, queueMaxSize)

}

class JoinOperator(id: String, ipe: Double, val reduction: Double, val window: Duration, queueMaxSize: Int)
    extends Operator(id, ipe, queueMaxSize)
    with InputVertex
    with OutputVertex {

  var reductionFactor = reduction

  override def init(startTime: Double = 0.0, simInterval: Double = 10.0): Unit = {
    val multiplier = window.div(Duration(simInterval, MILLISECONDS))
    reductionFactor = reduction * multiplier
  }


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
  private[query] def estimation(): Map[Vertex, Double] = {

    // get the first vertex
    val first = inputQueues.keysIterator.next()

    // calculate the relation (b.queueSize / first.queueSize) for all vertices b
    val proportion = inputQueues.map((elem) => (elem._1, elem._2 / inputQueues(first)))

    // multiply all relations, and multiply by the reduction factor
    val denominator = proportion.foldLeft(1.0)((accum, elem) => accum * elem._2) * reductionFactor

    val numerator = maximumNumberOfEvents

    // value is how many events the first vertex needs to process
    val value = Math.pow(numerator / denominator, 1.0 / inputQueues.size)

    // then, we calculate for each vertex
    proportion.map((elem) => (elem._1, value * elem._2))
  }

  override def run(instructions: Double, startTime: Double = 0.0): Double = {

    val events = retrieveFromInput(instructions, Vertex.sumOfValues(estimation()))

    // calculate the cartesian product among all input events
    val total = events.foldLeft(1.0)((accum, elem) => accum * elem._2)

    // the reduction parameter represents how much the join condition reduces
    // the number of joined elements
    sendToAllOutputs(total * reductionFactor)
    Vertex.sumOfValues(events)
  }

}
