package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.SimEvent

/** Trait for vertices of query graphs. */
trait Vertex {

  var queries: Set[Query] = Set.empty
  private[query] def addQuery(q: Query) = queries += q
  private[query] def removeQuery(q: Query) = queries -= q

  /** Vertex unique identifier. */
  def id: String

  /** Number of instructions needed to process one event. */
  def ipe: Double

  /** The number of instructions needed to process all pending events. */
  def instructionsNeeded: Double

  /** Indicates if the vertex has pending events and needs to be allocated. */
  def needsAllocation: Boolean = instructionsNeeded > 0.0

  // the next two methods are overriden in the InputVertex / OutputVertex traits.
  /**
   * Gets the set of successors of a vertex.
   * @return set of successors of a vertex.
   */
  def successors: Set[InputVertex] = Set.empty[InputVertex]

  /**
   * Gets the set of predecessors of a vertex.
   * @return set of predecessors of a vertex.
   */
  def predecessors: Set[OutputVertex] = Set.empty[OutputVertex]

  /**
    * Initializes the vertex.
    * @param startTime Initialization time (in milliseconds since the simulation start).
    * @param simInterval Simulation tick duration (in milliseconds).
    */
  def init(startTime: Double = 0.0, simInterval: Double = 10.0): Unit = { }

  /**
    * Executes the vertex logic.
    * @param instructions Number of allocated instructions.
    * @param startTime Time at which the simulation of this vertex has started (in ms since the simulation start).
    * @param endTime Time at which the simulation of this vertex will end (in ms since the simulation start).
    * @return A sequence of simulation events happened during the vertex simulation.
    */
  def run(instructions: Double, startTime: Double = 0.0, endTime: Double = 0.0): Seq[SimEvent]



  override def toString: String = s"[id: $id]"

  def compare(that: Vertex) = id.compare(that.id)
}

/** Vertex companion object. */
object Vertex {

  /** Vertex ordering definition. */
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
