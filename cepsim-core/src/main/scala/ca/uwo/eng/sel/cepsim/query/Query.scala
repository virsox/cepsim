package ca.uwo.eng.sel.cepsim.query

import java.util.{Map => JavaMap, Set => JavaSet}

import scala.collection.JavaConversions.{asScalaSet, mapAsScalaMap}


/** Query companion object */
object Query {
  
  def apply(id: String, vs: JavaSet[Vertex], es: JavaSet[(OutputVertex, InputVertex, Double)],
            duration: Long): Query =
      apply(id, asScalaSet(vs).toSet, asScalaSet(es).toSet, duration)
  
  
  def apply(id: String, vs: Set[Vertex], es: Set[(OutputVertex, InputVertex, Double)],
            duration: Long = Long.MaxValue): Query = {
    val q = new Query(id, Set.empty[Vertex], Map.empty[Vertex, Set[Edge]], duration)
    q addVertices(vs.toSeq:_*)
    q addEdges(es.toSeq:_*)
    q
  }
}


/**
  * A CEP query, composed of vertices and edges.
  *
  * @param id Unique query identifier.
  * @param v Set of query vertices.
  * @param e Map from vertices to a set of outgoing edges.
  * @param duration Query duration (for how long it runs).
  */
class Query protected (val id: String, v: Set[Vertex], e: Map[Vertex, Set[Edge]], val duration: Long) {

  /**
    * This is a private constructor used only by the Query companion object. Constructs a empty query.
    * @param id Query identifier.
    */
  private [query] def this(id: String) = this(id, Set.empty[Vertex], Map.empty[Vertex, Set[Edge]], Long.MaxValue)

  /**
    * This is constructor should be used when constructing a query from Java code.
    * @param id Query identifier.
    * @param v (Java) set of query vertices.
    * @param e (Java) map from vertices to a (Java) set of outgoing edges.
    * @param duration Query duration (for how long it runs).
    */
  def this(id: String, v: JavaSet[Vertex], e: JavaMap[Vertex, JavaSet[Edge]], duration: Long) = 
      this(id, asScalaSet(v).toSet,
           mapAsScalaMap(e).toMap.map((elem) => (elem._1, asScalaSet(elem._2).toSet[Edge])),
           duration)
  

  /** Set of query vertices. */
  var vertices: Set[Vertex] = v

  /** Map from vertices to its set of outgoing edges. */
  private var outgoingEdges: Map[Vertex, Set[Edge]] = e withDefaultValue(Set.empty)

  /** Map from vertices to its set of incoming edges. */
  private var incomingEdges: Map[Vertex, Set[Edge]] = {
    val tmpMap = scala.collection.mutable.Map[Vertex, Set[Edge]]() withDefaultValue(Set.empty)
    outgoingEdges.values.foreach{(set) =>
      set.foreach{(edge) =>
        val currentSet = tmpMap(edge.to)
        tmpMap(edge.to) = currentSet + edge
      }
    }
    tmpMap.toMap.withDefaultValue(Set.empty)
  }

  /** Set of query event producers. */
  def producers: Set[EventProducer] =
    vertices collect { case e: EventProducer => e }

  /** Set of query event consumers. */
  def consumers: Set[EventConsumer] =
    vertices collect { case e: EventConsumer => e }

  /**
    * Add a new vertex to the query.
    * @param v Vertex to be added.
    */
  def addVertex(v: Vertex) = addVertices(v)

  /**
    * Add a set of vertices to the query.
    * @param vs Set of vertices to be added.
    */
  def addVertices(vs: Vertex*) = {
    vertices = vertices ++ vs
    vs foreach (_.addQuery(this))

  }

  /**
    * Gets the set of vertex predecessors that belongs to this query.
    * @param v Vertex of which the predecessors are obtained.
    * @return set of vertex predecessors that belongs to this query.
    */
  def predecessors(v: Vertex): Set[OutputVertex] = incomingEdges(v).map(_.from.asInstanceOf[OutputVertex])

  /**
   * Gets the set of vertex successors that belongs to this query.
   * @param v Vertex of which the successors are obtained.
   * @return set of vertex successors that belongs to this query.
   */
  def successors(v: Vertex): Set[InputVertex] = outgoingEdges(v).map(_.to.asInstanceOf[InputVertex])

  /**
    * Gets the set edges starting from a vertex.
    * @param v Vertex of which the edges are obtained.
    * @return set edges starting from a vertex.
    */
  def edges(v: Vertex): Set[Edge] = outgoingEdges(v)

  /**
    * Obtains the Edge object representing the edge between two vertices.
    * @param from Origin vertex.
    * @param to Destination vertex.
    * @return Edge object representing the edge between two vertices.
    */
  def edge(from: Vertex, to: Vertex): Edge = outgoingEdges(from).find(_.to == to) match {
    case Some(e) => e
    case None    => throw new IllegalArgumentException
  }

  /**
    * Add a new edge to the query.
    * @param v1 Origin vertex
    * @param v2 Destination vertex.
    * @param selectivity Selectivity of the edge.
    */
  def addEdge(v1: OutputVertex, v2: InputVertex, selectivity: Double = 1.0): Unit =
    addEdges((v1, v2, selectivity))

  /**
    * Add a set of edges to the query.
    * @param es 3-tuples containing the origin vertex, destination vertex, and edge selectivity. Each tuple will
   *           be converted into an Edge object.
    */
  def addEdges(es: (OutputVertex, InputVertex, Double)*): Unit =
    addEdges(es.map((e) => Edge(e._1, e._2, e._3)).toSet)

  /**
    * Add a set of edges to the query.
    * @param es Set of edges to be added.
    */
  def addEdges(es: Set[Edge]): Unit = {
    es foreach { e =>
      e.from addOutputQueue (e.to, e.selectivity)
      e.to   addInputQueue  (e.from)
      outgoingEdges = outgoingEdges updated(e.from, outgoingEdges(e.from) + e)
      incomingEdges = incomingEdges updated(e.to, incomingEdges(e.to) + e)
    }
  }

  /**
    * Gets a list of paths from a specific consumer to query producers.
    * @param c Event consumer from which the paths are obtained.
    * @return list of paths from a specific consumer to query producers.
    */
  def pathsToProducers(c: EventConsumer): List[VertexPath] = pathsRecursive(c)

  /**
    * Auxiliary method of pathsToProducers.
    * @param vx Initial vertex.
    * @return List of paths from the initial vertex to the query event producers.
    */
  private def pathsRecursive(vx: Vertex): List[VertexPath] = {
    if (predecessors(vx).isEmpty) List(VertexPath(vx))
    else
      predecessors(vx).toList.sorted(Vertex.VertexIdOrdering).flatMap((pred) =>
        pathsRecursive(pred).map((vx, edge(pred, vx)) :: _)
      )
  }


}

