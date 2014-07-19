package ca.uwo.eng.sel.cepsim.query

import scala.collection.mutable.Queue


class Query(v: Set[Vertex], e: Map[Vertex, Set[Vertex]]) {


  def this() = this(Set.empty, Map.empty)
  
  def vertices: Set[Vertex] = v
  def producers: Set[Vertex] = v filter { case EventProducer(_) => true; case _ => false }
  def consumers: Set[Vertex] = v filter { case EventConsumer() => true; case _ => false }
  def edges: Map[Vertex, Set[Vertex]] = e withDefaultValue(Set.empty)
    
  
  def addVertex(v0: Vertex) = new Query(vertices + v0, edges)
  
  def addVertices(vs: Vertex*) = new Query(vertices ++ (vs), edges)
    
  def addEdge(v1: Vertex, v2: Vertex) =
    new Query(vertices, edges + (v1 -> (edges(v1) + v2)))
  
  def addEdges(es: (Vertex, Vertex)*) = {
    var newEdges = edges
    es foreach (p => newEdges = newEdges updated (p._1, newEdges(p._1) + p._2))
    new Query(vertices, newEdges)
  }
       

  def connections(v: Vertex): Set[Vertex] = edges(v)
  
  def iterator(): Iterator[Vertex] = {
    class VertexIterator extends Iterator[Vertex] {
      val queue: Queue[Vertex] = Queue(producers.toSeq:_*)

      def hasNext(): Boolean = !queue.isEmpty
      def next(): Vertex = {
        val v = queue.dequeue()
        // processing neighbours
        edges(v).foreach((n) => if (!queue.contains(n)) queue.enqueue(n))
        v
      }
    }
    new VertexIterator
  }

}