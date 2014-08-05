package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.Vm
import ca.uwo.eng.sel.cepsim.query.{Query, Vertex}

import scala.collection.mutable.Queue

/** Companion Placement object */
object Placement {
  def apply(q: Query, vm: Vm): Placement =
    new Placement(q, q.vertices, vm)
}

/** *
  * Represents a placement of query vertices into a virtual machine.
  * @param query Query to which the vertices belong.
  * @param vertices Set of vertices from this placement.
  * @param vm Virtual machine to which the vertices are assigned.
  */
class Placement(val query: Query, val vertices: Set[Vertex], val vm: Vm) extends Iterable[Vertex] {

  /**
    * Find all vertices from the placement that are producers, or do not have predecessors
    * that are in this same placement.
    * @return All start vertices.
    */
  def findStartVertices(): Set[Vertex] = {
    vertices.filter{(v) =>
      val predecessors = query.predecessors(v)
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
        query.successors(v).foreach { (n) =>
          if ((!queue.contains(n)) && (vertices.contains(v))) queue.enqueue(n)
        }
        v
      }
    }
    new VertexIterator
  }


}