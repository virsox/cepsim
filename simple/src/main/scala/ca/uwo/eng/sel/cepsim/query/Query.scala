package ca.uwo.eng.sel.cepsim.query

import scala.collection.mutable.Queue


object Query {
  def apply(vs: Set[Vertex], es: Set[(OutputVertex, InputVertex, Double)]) = {
    val q = new Query(vs.toSet, Map.empty)
    q addEdges(es toSeq:_*)
  }
}


class Query(v: Set[Vertex], e: Map[Vertex, Set[Edge]]) {

  // only for Unit testing
  private [query] def this() = this(Set.empty, Map.empty)
  
  val vertices: Set[Vertex] = v
  val producers: Set[Vertex] = vertices filter { case e: EventProducer => true; case _ => false }
  val consumers: Set[Vertex] = vertices filter { case e: EventConsumer => true; case _ => false }

  private val outgoingEdges: Map[Vertex, Set[Edge]] = e withDefaultValue(Set.empty)
  private val incomingEdges: Map[Vertex, Set[Edge]] = {
    val tmpMap = scala.collection.mutable.Map[Vertex, Set[Edge]]() withDefaultValue(Set.empty)
    outgoingEdges.values.foreach{(set) =>
      set.foreach{(edge) =>
        val currentSet = tmpMap(edge.to)
        tmpMap(edge.to) = currentSet + edge
      }
    }
    tmpMap.toMap.withDefaultValue(Set.empty)
  }


  
  def addVertex(v0: Vertex) = new Query(vertices + v0, outgoingEdges)
  def addVertices(vs: Vertex*) = new Query(vertices ++ (vs), outgoingEdges)

  def predecessors(v: Vertex): Set[Vertex] = incomingEdges(v).map(_.from)
  def successors(v: Vertex): Set[Vertex] = outgoingEdges(v).map(_.to)
  def edges(v: Vertex): Set[Edge] = outgoingEdges(v)


  def addEdge(v1: OutputVertex, v2: InputVertex): Query = addEdge(v1, v2, 1.0)
  def addEdge(v1: OutputVertex, v2: InputVertex, selectivity: Double): Query = addEdges((v1, v2, selectivity))

  def addEdges(es: (OutputVertex, InputVertex, Double)*): Query = {
    var newOutgoingEdges = outgoingEdges
    es foreach { p =>
      val edge = Edge(p._1, p._2, p._3)
      p._1 addOutputQueue (p._2, p._3)
      p._2 addInputQueue (p._1)
      newOutgoingEdges = newOutgoingEdges updated(p._1, newOutgoingEdges(p._1) + edge)
    }
    new Query(vertices, newOutgoingEdges)
  }
}