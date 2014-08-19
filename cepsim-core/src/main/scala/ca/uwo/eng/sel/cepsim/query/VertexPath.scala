package ca.uwo.eng.sel.cepsim.query

/**
 * Created by virso on 2014-07-31.
 */

object VertexPath {
  def apply(v: Vertex) = new VertexPath(List(v), Nil)
  def apply(vertices: List[Vertex], edges: List[Edge]) = new VertexPath(vertices, edges)
}

class VertexPath(val vertices: List[Vertex], val edges: List[Edge]) {
  def ::(p: (Vertex, Edge)) = VertexPath(p._1 :: vertices, p._2 :: edges)

  def producer = vertices.last.asInstanceOf[EventProducer]
  def weight: Double = edges.foldLeft(1.0)((acc, edge) => acc * edge.selectivity)
}
