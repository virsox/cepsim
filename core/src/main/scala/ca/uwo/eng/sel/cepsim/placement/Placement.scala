package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.Vm
import ca.uwo.eng.sel.cepsim.query.{EventProducer, Query, Vertex}

import scala.collection.mutable.Queue

/** Companion Placement object */
object Placement {
  def withQueries(queries: Set[Query], vm: Vm): Placement = new Placement(queries.flatMap(_.vertices), vm)

  def apply(q: Query, vm: Vm): Placement = new Placement(q.vertices, vm)
  def apply(vertices: Set[Vertex], vm: Vm): Placement = new Placement(vertices, vm)
}

/** *
  * Represents a placement of query vertices into a virtual machine.
  * @param vertices Set of vertices from this placement.
  * @param vm Virtual machine to which the vertices are assigned.
  */
class Placement(val vertices: Set[Vertex], val vm: Vm) extends Iterable[Vertex] {

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
  def addVertex(v: Vertex): Placement = new Placement(vertices + v, vm)

  /**
    * Get all queries that have at least one vertex in this placement.
    * @return queries that have at least one vertex in this placement.
    */
  def queries: Set[Query] = queryVerticesMap.keySet

  /**
    * Get all vertices in this placement from a specific query.
    * @param q query to which the vertices belong.
    * @return all vertices in this placement from a specific query.
    */
  def vertices(q: Query): Set[Vertex] = queryVerticesMap(q)

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