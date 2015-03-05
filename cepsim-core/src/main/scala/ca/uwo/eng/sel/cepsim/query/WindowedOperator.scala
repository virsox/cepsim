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
  var processAt: Double = 0.0

  var currentIndex = 0
  val slots = (size.toUnit(MILLISECONDS) / advance.toUnit(MILLISECONDS)).toInt
  var accumulated: Vector[Map[Vertex, Double]] =  Vector.fill(slots)(Map.empty withDefaultValue(0.0))


  override def init(startTime: Double = 0.0, simInterval: Double = 10.0): Unit = {
    start = startTime
    processAt = start + advance.toUnit(MILLISECONDS) //size.toUnit(MILLISECONDS)
  }

  private def accumulate(instructions: Double): Map[Vertex, Double] = {
    // retrieve events from input
    val retrievedEvents = retrieveFromInput(instructions, maximumNumberOfEvents)
    retrievedEvents.foreach((elem) => {
      accumulated = accumulated updated (currentIndex,
        accumulated(currentIndex) updated (elem._1, accumulated(currentIndex)(elem._1) + elem._2))
    })
    retrievedEvents
  }

  private def reset(index: Int): Unit = {
    accumulated = accumulated updated (index, Map.empty withDefaultValue(0.0))
  }

  private def totalAccumulated(): Map[Vertex, Double] = {
    accumulated.foldLeft(Map.empty[Vertex, Double])((acc, elem) => {
      elem.map((e) => (e._1, acc.getOrElse(e._1, 0.0) + e._2)) ++ acc.filterKeys(!elem.contains(_))
    })
  }


  override def run(instructions: Double, startTime: Double = 0.0): Double = {

    var processed = false
    var retrievedEvents = Map.empty[Vertex, Double]
    while (startTime >= processAt) {
      processAt = processAt + advance.toUnit(MILLISECONDS)

      // it is the last iteration - it should enqueue the events currently waiting
      if (processAt > startTime) retrievedEvents = accumulate(instructions)

      if (processAt > start + size.toUnit(MILLISECONDS)) {
        val total = totalAccumulated()
        if (Vertex.sumOfValues(total) > 0) {
          sendToAllOutputs(function(total))
        }
      }

      currentIndex = (currentIndex + 1) % slots
      reset(currentIndex)

      processed = true
    }

    if (!processed) retrievedEvents = accumulate(instructions)

    Vertex.sumOfValues(retrievedEvents)
//
//
//
//    while (startTime >= processAt) {
//      processAt = processAt + advance.toUnit(MILLISECONDS)
//      processed = true
//    }
//
//    // some window has passed
//    if ((processed) && (Vertex.sumOfValues(accumulated) > 0)) {
//      val output = function(accumulated)
//      sendToAllOutputs(output)
//      accumulated = Map.empty withDefaultValue(0.0)
//    }



  }

}
