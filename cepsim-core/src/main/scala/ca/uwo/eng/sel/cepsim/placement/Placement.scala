package ca.uwo.eng.sel.cepsim.placement

import java.util.{List => JavaList, Set => JavaSet}

import ca.uwo.eng.sel.cepsim.query._

import scala.collection.JavaConversions._
import scala.collection.mutable

/** Companion Placement object */
object Placement {
  // ----------------------- for java usage
  def apply(vertices: JavaSet[Vertex], vmId: Int): Placement = new Placement(asScalaSet(vertices).toSet, vmId)

  def withQueries(queries: JavaSet[Query], vmId: Int, iterationList:JavaList[Vertex]): Placement =
    Placement.withQueries(asScalaSet(queries).toSet, vmId, iterableAsScalaIterable(iterationList))

  def withQueries(queries: JavaSet[Query], vmId: Int): Placement =
    Placement.withQueries(asScalaSet(queries).toSet, vmId)

  // ----------------------------------------------------------------------------------------

  def withQueries(queries: Set[Query], vmId: Int, iterationOrder: Iterable[Vertex] = List.empty): Placement =
    new Placement(queries.flatMap(_.vertices), vmId, iterationOrder)

  def apply(q: Query, vmId: Int): Placement = new Placement(q.vertices, vmId)

  def apply(vertices: Set[Vertex], vmId: Int, iterationOrder: Iterable[Vertex] = List.empty): Placement =
    new Placement(vertices, vmId, iterationOrder)
}

/** *
  * Represents a placement of query vertices into a virtual machine.
  * @param vertices Set of vertices from this placement.
  * @param vmId Id of the Virtual machine to which the vertices are assigned.
  * @param itOrder Order on which vertices should be traversed. If not specified, vertices
  *                       are traversed according to a topological sorting of the query graphs.
  */
class Placement(val vertices: Set[Vertex], val vmId: Int, itOrder: Iterable[Vertex] = List.empty)
    extends Iterable[Vertex] {

  /** Map of queries to all vertices in this placement */
  var queryVerticesMap: Map[Query, Set[Vertex]] = Map.empty withDefaultValue Set.empty
  vertices foreach {(v) =>
    v.queries foreach {(q) =>
      queryVerticesMap = queryVerticesMap updated (q, queryVerticesMap(q) + v)
    }
  }

  val inPlacementSuccessors = vertices.map((v) => { (v,
    v.successors.filter((succ) => vertices.contains(succ)))
  }).toMap

  val notInPlacementSuccessors = vertices.map((v) => { (v,
    v.successors.filter((succ) => !vertices.contains(succ)))
  }).toMap


  /**
   * Get all queries that have at least one vertex in this placement.
   * @return queries that have at least one vertex in this placement.
   */
  val queries: Set[Query] = queryVerticesMap.keySet

  /**
   * Get all event producers in this placement.
   * @return all event producers in this placement.
   */
  val producers: Set[EventProducer] = vertices collect { case ep: EventProducer => ep }

  /**
   * Get all event consumers in this placement.
   * @return all event consumers in this placement.
   */
  val consumers: Set[EventConsumer] = vertices collect { case ec: EventConsumer => ec }


  /**
   * Get the execution duration of this placement (in seconds). It is calculated
   * as the maximum duration of all queries that belong to this placement.
   * @return Execution duration of this placement
   */
  val duration: Long = queries.foldLeft(0L){(max, query) =>
    (query.duration.max(max))
  }


  /**
    * Add a new vertex to the placement.
    * @param v Vertex to be added.
    * @return New placement with the vertex added.
    */
  def addVertex(v: Vertex): Placement = new Placement(vertices + v, vmId)

  /**
    * Return successors of a vertex that are in this placement.
    * @param v Vertex from which the successors are returned.
    * @return successors of a vertex in this placement.
    */
  def successorsInPlacement(v: Vertex): Set[InputVertex] = inPlacementSuccessors(v)


  /**
   * Return successors of a vertex that are not in this placement.
   * @param v Vertex from which the successors are returned.
   * @return successors of a vertex not in this placement.
   */
  def successorsNotInPlacement(v: Vertex): Set[InputVertex] = notInPlacementSuccessors(v)

  /**
    * Get the query with the informed id.
    * @param id Id of the query.
    * @return Optional of a query with the informed id.
    */
  def query(id: String): Option[Query] = queries.find(_.id == id)

  /**
    * Get all vertices in this placement from a specific query.
    * @param q query to which the vertices belong.
    * @return all vertices in this placement from a specific query.
    */
  def vertices(q: Query): Set[Vertex] = queryVerticesMap(q)

  /**
    * Find all vertices from the placement that are producers, or do not have predecessors
    * that are in this same placement.
    * @return All start vertices.
    */
  def findStartVertices(): Set[Vertex] = {
    vertices.filter{(v) =>
      val predecessors = v.predecessors.asInstanceOf[Set[Vertex]]
      predecessors.isEmpty || predecessors.intersect(vertices).isEmpty
    }
  }


  private def buildOrder: Iterable[Vertex] = {

    var index = 0
    var iterationOrder = Vector.empty[Vertex]

    if (!vertices.isEmpty) {

      var toProcess: Vector[Vertex] = Vector(findStartVertices().toSeq.sorted(Vertex.VertexIdOrdering):_*)
      var neighbours: mutable.Set[Vertex] = mutable.LinkedHashSet[Vertex]()

      while (index < toProcess.length) {
        val v = toProcess(index)
        iterationOrder = iterationOrder :+ v

        // processing neighbours (vertices that are still not in the list, but belong to this placement)
        v.successors.foreach { (successor) =>
          if ((!toProcess.contains(successor)) && (vertices.contains(successor))) neighbours.add(successor)
        }

        val toBeMoved = neighbours.filter((neighbour)  =>  neighbour.predecessors.forall(toProcess.contains(_)))
        neighbours = neighbours -- toBeMoved
        toProcess = toProcess ++ toBeMoved
        index += 1
      }
    }
    iterationOrder
  }


  val iterationOrder = if (!itOrder.isEmpty) itOrder else buildOrder

  /**
    * An iterator for this placement that traverse the vertices from the producers to consumers
    * in breadth-first manner.
    * @return Iterator that traverses all vertices from this placement.
    */
  override def iterator: Iterator[Vertex] = {
    class VertexIterator extends Iterator[Vertex] {

      val it = iterationOrder.iterator


      def hasNext(): Boolean = it.hasNext
      def next(): Vertex = it.next
    }

    new VertexIterator
  }


}