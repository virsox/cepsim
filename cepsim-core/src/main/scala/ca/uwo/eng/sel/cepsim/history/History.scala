package ca.uwo.eng.sel.cepsim.history

import ca.uwo.eng.sel.cepsim.query.Vertex

import scala.collection.mutable.ListBuffer

/** History companion object. */
object History {

  /**
    * Create a new empty History object.
    * @return empty History object.
    */
  def apply() = new History[SimEvent](List.empty[SimEvent])

  /**
    * Create a new History object from a sequence of simulation events.
    * @param entries Simulation events to be included in the History.
    * @tparam T Type of simulation events.
    * @return History object from the sequence.
    */
  def apply[T <: SimEvent](entries: Seq[T]) = new History[T](entries.sorted[SimEvent].toVector)


  /**
    * Implicit conversion from a Scala sequence to a History.
    * @param v Sequence being converted.
    * @tparam T Type of sequence elements.
    * @return History created from the sequence.
    */
  implicit def canBuildFrom[T <: SimEvent](v: Seq[T]) = new History[T](v)

}


/**
  * Represent execution history of one or more cloudlets. Entries should be logged in order,
  * and a vertex can only belong to a single cloudlet.
  * @param es Initial entries that are part of the History.
  * @tparam T Type of history entries.
  */
class History[T <: SimEvent](es: Seq[T]) extends Seq[T] {


  /** Contain the history simulation events. */
  private var buffer = ListBuffer.empty[T]

  // initialize buffer
  buffer ++= es

  /**
    * Construct an empty History.
    * @return Empty history.
    */
  def this() = this(List.empty)


  /**
   * Obtain simulation events from a specific vertex which occurs at (or after) the specified time.
   * @param v Vertex.
   * @param time Lower bound for the time when the events have occurred.
   * @return History containing all events that satisfy the specified filters.
   */
  def from(v: Vertex, time: Double): History[T] = from(v).filter(_.from >= time)

  /**
    * Obtain all simulation events from a specific vertex.
    * @param v Vertex.
    * @return History containing all events from a specific vertex.
    */
  def from(v: Vertex): History[T] = buffer.filter(_.v == v)

  /**
    * Log a sequence of simulation events.
    * @param simEvents Simulation events to be logged.
    * @return Reference to the history itself.
    */
  def log(simEvents: Seq[T]): History[T] = {
    simEvents.foreach(log(_))
    this
  }

  /**
   * Log a simulation event.
   * @param simEvent Simulation event to be logged.
   * @return Reference to the history itself.
   */
  def log(simEvent: T): History[T] = {
    buffer += simEvent
    this
  }

  /**
   * Merge the current history with the informed one.
   * @param other The history to be merged with.
   */
  def merge(other: History[T]) =
    if (!other.buffer.isEmpty) {
      if ((this.buffer.isEmpty) || (other.buffer.head.from >= this.buffer.last.from))
        this.buffer ++= other.buffer
      else
        this.buffer = (this.buffer ++ other.buffer).sorted[SimEvent]
    }


  // -------------- Methods from the Seq interface
  override def length: Int = buffer.length
  override def apply(idx: Int): T = buffer.apply(idx)
  override def iterator: Iterator[T] = buffer.iterator

}
