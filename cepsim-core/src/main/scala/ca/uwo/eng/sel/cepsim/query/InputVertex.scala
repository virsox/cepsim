package ca.uwo.eng.sel.cepsim.query

import scala.collection.immutable.TreeMap

/**
 * Created by virso on 2014-07-24.
 */
trait InputVertex extends Vertex  { this: Vertex =>

  var inputQueues: Map[Vertex, Double] = TreeMap[Vertex, Double]()(Vertex.VertexIdOrdering)

  val queueMaxSize: Int
  def isBounded() = queueMaxSize > 0


  def initInputQueues(predecessors: Set[Vertex]) = {
    predecessors.foreach(addInputQueue(_))
  }

  def addInputQueue(v: Vertex) = {
    inputQueues = inputQueues + (v -> 0)
  }

  def totalInputEvents = Vertex.sumOfValues(inputQueues)



  def enqueueIntoInput(v: Vertex, x: Double): Double = {
    inputQueues = inputQueues updated(v, inputQueues(v) + x)
    inputQueues(v)
  }

  protected def dequeueFromInput(pairs: (Vertex, Double)*) =
    pairs.foreach {(pair) =>
      inputQueues = inputQueues updated(pair._1, inputQueues(pair._1) - pair._2)
    }


  override def predecessors: Set[OutputVertex] = queries.flatMap(_.predecessors(this))

}
