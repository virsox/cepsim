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
  val producers: Set[EventProducer] = {
    var tmpProducers: Set[EventProducer] = Set.empty
    vertices.foreach{case e: EventProducer => tmpProducers += e; case _ => ;}
    tmpProducers
  }
  val consumers: Set[EventConsumer] = {
    var tmpConsumers: Set[EventConsumer] = Set.empty
    vertices.foreach{case e: EventConsumer => tmpConsumers += e; case _ => ;}
    tmpConsumers
  }

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
  def edge(from: Vertex, to: Vertex): Edge = outgoingEdges(from).find(_.to == to) match {
    case Some(e) => e
    case None    => throw new IllegalArgumentException
  }


  //def addEdge(v1: OutputVertex, v2: InputVertex): Query = addEdge(v1, v2, 1.0)
  def addEdge(v1: OutputVertex, v2: InputVertex, selectivity: Double = 1.0): Query = addEdges((v1, v2, selectivity))

  def addEdges(es: (OutputVertex, InputVertex, Double)*): Query =
    addEdges(es.map((e) => Edge(e._1, e._2, e._3)).toSet)

  def addEdges(es: Set[Edge]): Query = {
    var newOutgoingEdges = outgoingEdges
    es foreach { e =>
      e.from addOutputQueue (e.to, e.selectivity)
      e.to   addInputQueue  (e.from)
      newOutgoingEdges = newOutgoingEdges updated(e.from, newOutgoingEdges(e.from) + e)
    }
    new Query(vertices, newOutgoingEdges)
  }


  // ---------------------------------------------
  def pathsToProducers(c: EventConsumer): List[VertexPath] = {
    pathsRecursive(c)
  }

  private def pathsRecursive(vx: Vertex): List[VertexPath] = {
    if (predecessors(vx).isEmpty) List(VertexPath(vx))
    else
      predecessors(vx).toList.sorted(Vertex.VertexIdOrdering).flatMap((pred) =>
        pathsRecursive(pred).map((vx, edge(pred, vx)) :: _)
      )
  }

}