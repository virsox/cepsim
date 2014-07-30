package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.query.{Query, Vertex}

import scala.collection.mutable.Queue

object Placement {
  def apply(q: Query, vmId: Long): Placement =
    new Placement(q, q.vertices, vmId)
}

class Placement(val query: Query, val vertices: Set[Vertex], val vmId: Long) extends Iterable[Vertex] {

  def findStartVertices(): Set[Vertex] = {
    vertices.filter{(v) =>
      val predecessors = query.predecessors(v)
      predecessors.isEmpty || predecessors.intersect(vertices).isEmpty
    }
  }

  override def iterator: Iterator[Vertex] = {
    class VertexIterator extends Iterator[Vertex] {
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

      //override def toMap[K, V](implicit ev: <:<[Vertex, (K, V)]): GenMap[K, V] = ???
    }
    new VertexIterator
  }


}