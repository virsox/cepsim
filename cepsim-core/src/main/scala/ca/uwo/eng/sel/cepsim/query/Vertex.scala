package ca.uwo.eng.sel.cepsim.query

trait Vertex {

  var queries: Set[Query] = Set.empty



  def id: String
  def ipe: Double


  private[query] def addQuery(q: Query) = queries += q
  private[query] def removeQuery(q: Query) = queries -= q

  def run(instructions: Double): Int


  override def toString: String = s"[id: $id]"

  def compare(that: Vertex) = id.compare(that.id)

  def sumOfValues(map: Map[Vertex, Int]): Int = map.foldLeft(0)((sum, elem) => sum + elem._2)


}

object Vertex {
  implicit object VertexIdOrdering extends Ordering[Vertex] {
    override def compare(x: Vertex, y: Vertex): Int = {
      x.id.compare(y.id)
    }
  }


}
