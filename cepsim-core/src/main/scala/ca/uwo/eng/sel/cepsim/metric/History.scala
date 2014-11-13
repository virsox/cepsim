package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.query.{Operator, EventProducer, Vertex}

/** Companion object. */
object History {
  def apply() = new History(List.empty[Entry])

  /** An entry in the cloudlet execution history. */
  trait Entry extends Ordered[Entry] {
    /** Cloudlet to which the entry belongs. */
    def cloudlet: String

    /** Timestamp of the entry. */
    def time: Double

    /** Vertex. */
    def v: Vertex

    def quantity: Int



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
  case class Processed(cloudlet: String, time: Double, v: Vertex, quantity: Int) extends Entry {
    def reduceBy(value: Int): Processed = new Processed(cloudlet, time, v, quantity - value)
  }

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


  implicit def canBuildFrom[T <: Entry](v: Vector[T]) = new History(v)
  implicit def canBuildFrom[T <: Entry](v: Seq[T])    = new History(v.toList)

}

import ca.uwo.eng.sel.cepsim.metric.History._

/** 
  * Represent execution history of one or more cloudlets. Entries should be logged in order,
  * and a vertex belong to a single cloudlet.
  */
class History[T <: Entry] private (es: Vector[T]) extends IndexedSeq[T] {

  //def this(es: Vector[Entry])
  def this(list: List[T]) = this(Vector.empty ++ list)


  /** vector of entries */
  var history: Vector[T] = Vector.empty ++ es

  /**
    * Obtain all entries as a list.
    * @return all entries as a list.
    */
  //def entries(): List[T] = history.toList

  /**
    * Obtain the last entry from a specific vertex.
    * @return Optional last entry of a vertex.  
    */
  def lastFrom(v: Vertex): Option[T] = from(v) match {
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
    * @return entries from a specific vertex.
    */
  def from(v: Vertex): List[T] = history.filter(_.v == v).toList

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
   * @return processing entries from a specific vertex.
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
    */
  def logProcessed(cloudlet: String, time: Double, v: Vertex, quantity: Int) =
    new History[Entry](history :+ Processed(cloudlet, time, v, quantity))

  /**
    * Log a new Received entry in the history.
    * @param cloudlet Cloudlet to which this entry belongs.
    * @param time Timestamp of the entry.
    * @param v Vertex.
    * @param orig Origin vertex.
    * @param quantity Quantity of events received from the origin vertex.
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

  //override def iterator: Iterator[Entry] = history.iterator
  override def length: Int = history.length
  override def apply(idx: Int): T = history.apply(idx)
}
