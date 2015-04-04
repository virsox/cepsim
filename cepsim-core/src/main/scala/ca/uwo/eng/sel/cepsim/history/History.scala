package ca.uwo.eng.sel.cepsim.history

import ca.uwo.eng.sel.cepsim.query.Vertex

/** Companion object. */
object History {

  /**
    * Create a new empty History object.
    * @return empty History object.
    */
  def apply() = new History[Entry](List.empty[Entry])

  /**
    * Create a new History object from a sequence of entries.
    * @param entries Entries to be included in the History.
    * @tparam T Type of entries.
    * @return History object from the sequence.
    */
  def apply[T <: Entry](entries: Seq[T]) = new History[T](entries.sorted[Entry].toVector)

  /** An entry in the cloudlet execution history. */
  trait Entry extends Ordered[Entry] {
    /** Cloudlet to which the entry belongs. */
    def cloudlet: String

    /** Timestamp of the entry. */
    def time: Double

    /** Vertex. */
    def v: Vertex

    /** Processed events. */
    //def quantity: Double

    def compare(that: Entry): Int = {
      val timeComp = this.time.compare(that.time)
      if (timeComp == 0) this.cloudlet.compare(that.cloudlet)
      else timeComp
    }
  }

  /**
    * A history entry representing events processed.
    * @param cloudlet Cloudlet to which the entry belongs.
    * @param time Timestamp of the entry.
    * @param v Vertex.
    * @param quantity Quantity of events processed by the vertex.
    */
  case class Processed(cloudlet: String, time: Double, v: Vertex, quantity: Double) extends Entry

  /**
    * * A history entry representing events sent to a remote vertex.
    * @param cloudlet Cloudlet to which the entry belongs.
    * @param time Timestamp of the entry.
    * @param v Vertex.
    * @param dest Destination vertex.
    * @param quantity Quantity of events sent from v to dest.
    */
  case class Sent(cloudlet: String, time: Double, v: Vertex, dest: Vertex, quantity: Int) extends Entry

  /**
    * A history entry representing events received from a remote vertex.
    * @param cloudlet Cloudlet to which the entry belongs.
    * @param time Timestamp of the entry.
    * @param v Vertex.
    * @param orig Origin vertex.
    * @param quantity Quantity of events received from orig.
    */
  case class Received(cloudlet: String, time: Double, v: Vertex, orig: Vertex, quantity: Int) extends Entry

  /**
    * Implicit conversion from a sequence to a History.
    * @param v Sequence being converted.
    * @tparam T Type of sequence elements.
    * @return History created from the sequence.
    */
  implicit def canBuildFrom[T <: Entry](v: Seq[T]) = new History[T](v.toList)

}

import ca.uwo.eng.sel.cepsim.history.History._


/**
  * Represent execution history of one or more cloudlets. Entries should be logged in order,
  * and a vertex belong to a single cloudlet.
  * @param es Initial entries that are part of the History.
  * @tparam T Type of history entries.
  */
class History[T <: Entry] private (es: Vector[T]) extends Seq[T] {


  /**
    * Construct a History from a list.
    * @param list List of initial entries.
    * @return History constructed from the list.
    */
  def this(list: List[T]) = this(Vector.empty ++ list)

  /**
    * Construct an empty History.
    * @return Empty history.
    */
  def this() = this(Vector.empty)


  /** vector of entries */
  var history: Vector[T] = Vector.empty ++ es

  /**
    * Obtain the last entry from a specific vertex.
    * @return Optional last entry of a vertex.  
    */
  def lastFrom(v: Vertex): Option[T] = from(v) toList match {
  	case Nil => None
	  case list => Option(list.last)
  }
  
  /**
   * Obtain the first entry from a specific vertex which occurs at (or after) the specified time.
   * @param v Vertex.
   * @param time Lower bound for the time when the entry occurred.
   * @return (Optional) Entry that satisfy the specified filters.
   */
  def from(v: Vertex, time: Double): Option[T] = from(v).find(_.time >= time)

  /**
    * Obtain entries from a specific vertex.
    * @param v Vertex of the entries.
    * @return History with entries from a specific vertex.
    */
  def from(v: Vertex): History[T] = history.filter(_.v == v)

  /**
   * Obtain the first processing entry from a specific vertex which occurs at (or after) the specified time.
   * @param v Vertex.
   * @param time Lower bound for the time when the entry occurred.
   * @return (Optional) Processing entry that satisfy the specified filters.
   */
  def processedEntriesFrom(v: Vertex, time: Double): Option[Processed] = processedEntriesFrom(v).find(_.time >= time)

  /**
   * Obtain all processing entries from a specific vertex.
   * @param v Vertex of the entries.
   * @return History with processing entries from a specific vertex.
   */
  def processedEntriesFrom(v: Vertex): History[Processed] = from(v).collect{ case e: Processed => e }


  /** *
    * Obtain the entry from the same cloudlet succeeding the informed one.
    * @param entry History entry that the successor is being looked for.
    * @return Optional entry succeeding the informed one.
    */
  def successor(entry: Entry): Option[T] = {
    val index = history.indexOf(entry)
    if ((index == -1) || (index == history.length - 1)) None
    else history.drop(index + 1).find(_.cloudlet == entry.cloudlet)
  }

  /**
    * Log a new Processed entry in the history.
    * @param cloudlet Cloudlet to which this entry belongs.
    * @param time Timestamp of the entry.
    * @param v Vertex.
    * @param quantity Quantity of events processed by the vertex.
    * @return History appended with the entry.
    */
  def logProcessed(cloudlet: String, time: Double, v: Vertex, quantity: Double) =
    new History[Entry](history :+ Processed(cloudlet, time, v, quantity))

  /**
    * Log a new Received entry in the history.
    * @param cloudlet Cloudlet to which this entry belongs.
    * @param time Timestamp of the entry.
    * @param v Vertex.
    * @param orig Origin vertex.
    * @param quantity Quantity of events received from the origin vertex.
    * @return History appended with the entry.
    */
  def logReceived(cloudlet: String, time: Double, v: Vertex, orig: Vertex, quantity: Int) =
    new History[Entry](history :+ Received(cloudlet, time, v, orig, quantity))


  /**
   * Log a new Sent entry in the history.
   * @param cloudlet Cloudlet to which this entry belongs.
   * @param time Timestamp of the entry.
   * @param v Vertex.
   * @param dest Destination vertex.
   * @param quantity Quantity of events sent to the destination vertex.
   * @return History appended with the entry.
   */
  def logSent(cloudlet: String, time: Double, v: Vertex, dest: Vertex, quantity: Int) =
    new History[Entry](history :+ Sent(cloudlet, time, v, dest, quantity))

  /**
   * Merge the current history with the informed one.
   * @param other The history to be merged with.
   * @return a new history containing entries from both histories.
   */
  def merge(other: History[Entry]) =
    new History((this.history ++ other.history).sorted.toList)

  /**
    * Add a new entry to the history. It is used by the LatencyMetric calculation only.
    * @param entry Entry to be added.
    * @return A History with the new entry added in the right place.
    */
  private[history] def add(entry: T): History[T] =
    new History[T]((this.history :+ entry).sorted[Entry])

  /**
    * Remove entries from the History. It is used by the LatencyMetric calculation only.
    * @param entries entries to be removed.
    * @return A new history without the informed entries.
    */
  private[history] def remove(entries: T*): History[T] =
    new History(history.filterNot(entries.contains(_)))


  // -------------- Methods from the Seq interface
  override def length: Int = history.length
  override def apply(idx: Int): T = history.apply(idx)
  override def iterator: Iterator[T] = history.iterator

}
