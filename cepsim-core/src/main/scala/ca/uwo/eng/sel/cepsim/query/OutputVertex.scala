package ca.uwo.eng.sel.cepsim.query

import scala.collection.immutable.TreeMap

/**
 * Created by virso on 2014-07-24.
 */
trait OutputVertex extends Vertex { //this: Vertex =>

  var outputQueues: Map[Vertex, Double] = TreeMap[Vertex, Double]()(Vertex.VertexIdOrdering)
  var selectivities: Map[Vertex, Double] = Map.empty
  var accumulators: Map[Vertex, Double] = Map.empty

  var limits: Map[Vertex, Double] = Map.empty

  def initOutputQueues(successors: Set[Vertex]) = {
    successors.foreach(addOutputQueue(_))
  }

  def addOutputQueue(v: Vertex, selectivity: Double = 1.0) = {
    outputQueues = outputQueues + (v -> 0)
    selectivities = selectivities + (v -> selectivity)
    accumulators = accumulators + (v -> 0)
    limits = limits + (v -> Int.MaxValue)
  }

  def setLimit(v: Vertex, limit: Double) = {
    limits = limits updated (v, limit)
  }

  /**
    * Obtains the maximum number of events that can be produced by the vertex (it is calculated
    * as minimum among all limited output buffers taking the output selectivity into account).
    * @return maximum number of events that can be produced by the vertex.
    */
  def maximumNumberOfEvents: Double = {
    limits.map((elem) => (elem._1, elem._2 / selectivities(elem._1))).
                      foldLeft(Double.MaxValue)((acc, elem) => acc.min(elem._2))
  }


  def dequeueFromOutput(pairs: (Vertex, Double)*) =
    pairs.foreach {(pair) =>
      outputQueues = outputQueues updated(pair._1, outputQueues(pair._1) - pair._2)
    }


  def sendToAllOutputs(x: Double): Map[Vertex, Double] = {
    sendToOutputs(selectivities.mapValues((elem) => x))
  }

  def sendToOutputs(outputs: Map[Vertex, Double]): Map[Vertex, Double] = {
//    var withSelectivity = outputs.map{(elem) =>
//      (elem._1, Math.floor(elem._2 * selectivities(elem._1)).toInt)
//    }
    val withSelectivity = outputs.map{(elem) =>
      (elem._1, elem._2 * selectivities(elem._1))
    }

//    val decimal = outputs.map{(elem) =>
//      (elem._1, elem._2 * selectivities(elem._1) - withSelectivity(elem._1))
//    }
//
//    decimal.foreach{(elem) =>
//      if (elem._2 > 0.01) {
//        accumulators = accumulators updated (elem._1, accumulators(elem._1) + elem._2)
//        if (accumulators(elem._1) >= 1) {
//          accumulators = accumulators updated (elem._1, accumulators(elem._1) - 1)
//          withSelectivity = withSelectivity updated (elem._1, withSelectivity(elem._1) + 1)
//        }
//      }
//    }

    outputQueues = outputQueues.map{(elem) =>
      (elem._1, elem._2 + withSelectivity.getOrElse(elem._1, 0.0))
    }
    withSelectivity
  }

}
