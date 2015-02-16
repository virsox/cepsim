package ca.uwo.eng.sel.cepsim.query

import scala.concurrent.duration._


object WindowedOperator {

  def apply(id: String, ipe: Double, size: Double, advance: Double, function: (Map[Vertex, Double]) => Double) =
    new WindowedOperator(id, ipe, size milliseconds, advance milliseconds, function, 1000)

  def identity(): (Map[Vertex, Double]) => Double = ((x) => Vertex.sumOfValues(x))
  def constant(c: Double): (Map[Vertex, Double]) => Double = ((x) => c)

}

/**
 * Created by virso on 14-12-03.
 */
class WindowedOperator(id: String, ipe: Double, size: Duration, advance: Duration,
                      function: (Map[Vertex, Double]) => Double, queueMaxSize: Int)
  extends Operator(id, ipe, queueMaxSize) {

  var start = 0.0
  var processAt: Stream[Double] = Stream.empty
  var accumulated: Map[Vertex, Double] = Map.empty withDefaultValue(0.0)

  override def init(startTime: Double = 0.0, simInterval: Double = 10.0): Unit = {
    start = startTime

    def next(n: Double): Stream[Double] = n #:: next(n + advance.toUnit(MILLISECONDS))
    processAt = next(start + size.toUnit(MILLISECONDS))
  }

  override def run(instructions: Double, startTime: Double = 0.0): Double = {


    // retrieve events from input
    val retrievedEvents = retrieveFromInput(instructions, maximumNumberOfEvents)
    retrievedEvents.foreach((elem) => {
      accumulated = accumulated updated (elem._1, accumulated(elem._1) + elem._2)
    })

    var processed = false
    while (startTime >= processAt.head) {
      processAt = processAt.tail
      processed = true
    }

    // some window has passed
    if ((processed) && (Vertex.sumOfValues(accumulated) > 0)) {
      val output = function(accumulated)
      sendToAllOutputs(output)
      accumulated = Map.empty withDefaultValue(0.0)
    }


    Vertex.sumOfValues(retrievedEvents)
  }

}
