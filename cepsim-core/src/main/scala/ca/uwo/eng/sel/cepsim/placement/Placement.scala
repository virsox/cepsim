package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.Vm
import ca.uwo.eng.sel.cepsim.query.{EventProducer, Query, Vertex}

import scala.collection.mutable.Queue

import java.util.{Set => JavaSet}
import scala.collection.JavaConversions.asScalaSet

/** Companion Placement object */
object Placement {
  def withQueries(queries: JavaSet[Query], vmId: Int): Placement = Placement.withQueries(asScalaSet(queries).toSet, vmId)
  def withQueries(queries: Set[Query], vmId: Int): Placement = new Placement(queries.flatMap(_.vertices), vmId)
  def apply(q: Query, vmId: Int): Placement = new Placement(q.vertices, vmId)
  def apply(vertices: Set[Vertex], vmId: Int): Placement = new Placement(vertices, vmId)
}

/** *
  * Represents a placement of query vertices into a virtual machine.
  * @param vertices Set of vertices from this placement.
  * @param vmId Id of the Virtual machine to which the vertices are assigned.
  */
class Placement(val vertices: Set[Vertex], val vmId: Int) extends Iterable[Vertex] {

  /** Map of queries to all vertices in this placement */
  var queryVerticesMap: Map[Query, Set[Vertex]] = Map.empty withDefaultValue Set.empty
  vertices foreach {(v) =>
    v.queries foreach {(q) =>
      queryVerticesMap = queryVerticesMap updated (q, queryVerticesMap(q) + v)
    }
  }

  /**
    * Add a new vertex to the placement.
    * @param v Vertex to be added.
    * @return New placement with the vertex added.
    */
  def addVertex(v: Vertex): Placement = new Placement(vertices + v, vmId)

  /**
    * Get all queries that have at least one vertex in this placement.
    * @return queries that have at least one vertex in this placement.
    */
  def queries: Set[Query] = queryVerticesMap.keySet

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
    * Get the execution duration of this placement (in seconds). It is calculated
    * as the maximum duration of all queries that belong to this placement.
    * @return Execution duration of this placement
    */
  def duration: Long = queries.foldLeft(0L){(max, query) =>
    (query.duration.max(max))
  }
  
  /**
    * Get all event producers in this placement.
    * @return all event producers in this placement.
    */
  def producers: Set[EventProducer] = vertices collect { case ep: EventProducer => ep }

  /**
    * Find all vertices from the placement that are producers, or do not have predecessors
    * that are in this same placement.
    * @return All start vertices.
    */
  def findStartVertices(): Set[Vertex] = {
    vertices.filter{(v) =>
      var predecessors = Set.empty[Vertex]
      v.queries foreach{(q) =>
        predecessors = predecessors ++ q.predecessors(v)
      }
      predecessors.isEmpty || predecessors.intersect(vertices).isEmpty
    }
  }

  /**
    * An iterator for this placement that traverse the vertices from the producers to consumers
    * in breadth-first manner.
    * @return Iterator that traverses all vertices from this placement.
    */
  override def iterator: Iterator[Vertex] = {
    class VertexIterator extends Iterator[Vertex] {

      /** FIFO queue */
      val queue: Queue[Vertex] = Queue(findStartVertices().toSeq.sorted(Vertex.VertexIdOrdering):_*)

      def hasNext(): Boolean = !queue.isEmpty
      def next(): Vertex = {
        val v = queue.dequeue()
        // processing neighbours
        v.queries.foreach {(q) =>
          q.successors(v).foreach { (n) =>
            if ((!queue.contains(n)) && (vertices.contains(v))) queue.enqueue(n)
          }
        }
        v
      }
    }
    new VertexIterator
  }


}