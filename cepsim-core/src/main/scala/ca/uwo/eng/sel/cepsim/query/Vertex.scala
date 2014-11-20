package ca.uwo.eng.sel.cepsim.query

trait Vertex {

  var queries: Set[Query] = Set.empty



  def id: String
  def ipe: Double


  private[query] def addQuery(q: Query) = queries += q
  private[query] def removeQuery(q: Query) = queries -= q

  def init(startTime: Double = 0.0): Unit = { }
  def run(instructions: Double): Double


  override def toString: String = s"[id: $id]"

  def compare(that: Vertex) = id.compare(that.id)

  /**
    * Calculate the sum of all values of the informed map.
    * @param map Map from which the values will be summed.
    * @return The sum of all values contained in the map.
    */
  def sumOfValues(map: Map[Vertex, Double]): Double = map.foldLeft(0.0)((sum, elem) => sum + elem._2)


}

object Vertex {
  implicit object VertexIdOrdering extends Ordering[Vertex] {
    override def compare(x: Vertex, y: Vertex): Int = {
      x.id.compare(y.id)
    }
  }


}
