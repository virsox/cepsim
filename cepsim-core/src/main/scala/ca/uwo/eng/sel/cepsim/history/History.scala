package ca.uwo.eng.sel.cepsim.history

import ca.uwo.eng.sel.cepsim.query.Vertex

import scala.collection.mutable.ListBuffer

/** Companion object. */
object History {

  /**
    * Create a new empty History object.
    * @return empty History object.
    */
  def apply() = new History[SimEvent](List.empty[SimEvent])

  /**
    * Create a new History object from a sequence of entries.
    * @param entries Entries to be included in the History.
    * @tparam T Type of entries.
    * @return History object from the sequence.
    */
  def apply[T <: SimEvent](entries: Seq[T]) = new History[T](entries.sorted[SimEvent].toVector)


  /**
    * Implicit conversion from a sequence to a History.
    * @param v Sequence being converted.
    * @tparam T Type of sequence elements.
    * @return History created from the sequence.
    */
  implicit def canBuildFrom[T <: SimEvent](v: Seq[T]) = new History[T](v)

}


/**
  * Represent execution history of one or more cloudlets. Entries should be logged in order,
  * and a vertex belong to a single cloudlet.
  * @param es Initial entries that are part of the History.
  * @tparam T Type of history entries.
  */
class History[T <: SimEvent](es: Seq[T]) extends Seq[T] {


  /** Contain the history entries. */
  private val buffer = ListBuffer.empty[T]

  // initialize buffer
  buffer ++= es

  /**
    * Construct an empty History.
    * @return Empty history.
    */
  def this() = this(List.empty)


  /**
   * Obtain the first entry from a specific vertex which occurs at (or after) the specified time.
   * @param v Vertex.
   * @param time Lower bound for the time when the entry occurred.
   * @return (Optional) Entry that satisfy the specified filters.
   */
  def from(v: Vertex, time: Double): History[T] = from(v).filter(_.from >= time)

  /**
    * Obtain entries from a specific vertex.
    * @param v Vertex of the entries.
    * @return History with entries from a specific vertex.
    */
  def from(v: Vertex): History[T] = buffer.filter(_.v == v)



  def log(simEvent: T) = buffer += simEvent
    //new History[Entry](history :+ Processed(cloudlet, time, v, quantity))



  /**
   * Merge the current history with the informed one.
   * @param other The history to be merged with.
   * @return a new history containing entries from both histories.
   */
  def merge(other: History[SimEvent]) =
    new History((this.buffer ++ other.buffer).sorted.toList)



  // -------------- Methods from the Seq interface
  override def length: Int = buffer.length
  override def apply(idx: Int): T = buffer.apply(idx)
  override def iterator: Iterator[T] = buffer.iterator

}
