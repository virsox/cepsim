package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.SimEvent

/** Trait for vertices of query graphs. */
trait Vertex {

  var queries: Set[Query] = Set.empty
  private[query] def addQuery(q: Query) = queries += q
  private[query] def removeQuery(q: Query) = queries -= q

  def id: String
  def ipe: Double

  /**
    * Initializes the vertex.
    * @param startTime Initialization time (in milliseconds since the simulation start).
    * @param simInterval Simulation tick duration (in milliseconds).
    */
  def init(startTime: Double = 0.0, simInterval: Double = 10.0): Unit = { }

  /**
    * Executes the vertex.
    * @param instructions Number of allocated instructions.
    * @param startTime Time at which the method run has been invoked (in milliseconds since the simulation start).
    * @return Number of processed events.
    */
  def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent]


  override def toString: String = s"[id: $id]"

  def compare(that: Vertex) = id.compare(that.id)

  // must be overriden
  def successors: Set[InputVertex] = Set.empty[InputVertex]
  def predecessors: Set[OutputVertex] = Set.empty[OutputVertex]



}

object Vertex {
  implicit object VertexIdOrdering extends Ordering[Vertex] {
    override def compare(x: Vertex, y: Vertex): Int = {
      x.id.compare(y.id)
    }
  }

  /**
   * Calculate the sum of all values of the informed map.
   * @param map Map from which the values will be summed.
   * @return The sum of all values contained in the map.
   */
  def sumOfValues[T](map: Map[T, Double]): Double = map.foldLeft(0.0)((sum, elem) => sum + elem._2)

}
