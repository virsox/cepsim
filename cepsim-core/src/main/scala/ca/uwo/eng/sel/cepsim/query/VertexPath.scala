package ca.uwo.eng.sel.cepsim.query

/** VertexPath companion object. */
object VertexPath {
  def apply(v: Vertex) = new VertexPath(List(v), Nil)
  def apply(vertices: List[Vertex], edges: List[Edge]) = new VertexPath(vertices, edges)
}

/**
  * Encapsulates a path from a vertex to an event producer.
  * @param vertices Sequence of vertices that are part of the path.
  * @param edges List of edges that are part of the path.
  */
class VertexPath(val vertices: List[Vertex], val edges: List[Edge]) {
  /**
    * Concatenates a new vertex and edge to the path.
    * @param p Tuples containing the vertex and edge to be added.
    * @return A new path containing the added vertex / edge.
    */
  def ::(p: (Vertex, Edge)) = VertexPath(p._1 :: vertices, p._2 :: edges)

  /**
    * Returns the last vertex from the path (an event producer).
    * @return last vertex from the path (an event producer).
    */
  def producer = vertices.last.asInstanceOf[EventProducer]

  /**
    * Calculates the path weight (product of all edge selectivies that compose the path).
    * @return path weight (product of all edge selectivies that compose the path).
    */
  def weight: Double = edges.foldLeft(1.0)((acc, edge) => acc * edge.selectivity)
}
