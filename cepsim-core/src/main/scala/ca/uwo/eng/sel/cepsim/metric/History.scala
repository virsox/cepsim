package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.query.Vertex

/** Companion object. */
object History {
  def apply() = new History(List.empty[Entry])

  /***
    * An entry in the cloudlet execution history
    * @param cloudlet Cloudlet to which this entry belongs.
    * @param time Timestamp of the entry.
    * @param v Vertex.
    * @param quantity Quantity of events processed by the vertex.
    */
  case class Entry(cloudlet: String, time: Double, v: Vertex, quantity: Int) extends Ordered[Entry] {
    def compare(that: Entry): Int = {
      val timeComp = this.time.compare(that.time)
      if (timeComp == 0) this.cloudlet.compare(that.cloudlet)
      else timeComp
    }
  }

}

import ca.uwo.eng.sel.cepsim.metric.History._

/** 
  * Represent execution history of one or more cloudlets. Entries should be logged in order,
  * and a vertex belong to a single cloudlet.
  */
class History(es: List[Entry]) {

  /** vector of entries */
  var history: Vector[Entry] = Vector.empty ++ es

  /**
    * Obtain all entries as a list.
    * @return all entries as a list.
    */
  def entries(): List[Entry] = history.toList

  
  /**
   * Obtain the first entry from a specific vertex which occurs at (or after) the specified time.
   * @param v Vertex.
   * @param time Lower bound for the time when the entry occurred.
   * @return (Optional) Entry that satisfy the specified filters.
   */
  def from(v: Vertex, time: Double): Option[Entry] = from(v).find(_.time >= time)
  
//  /**
//   * Obtain the entries from a specific cloudlet and vertex.
//   * @param cloudlet Name of the cloudlet.
//   * @param v Vertex.
//   * @return All the entries of the specified cloudlet and vertex.
//   */
//  def from(cloudlet: String, v: Vertex): List[Entry] =
//    history.filter((e) => (e.cloudlet == cloudlet) && (e.v == v)).toList

  /**
    * Obtain entries of a specific vertex.
    * @param v Vertex of the entries.
    * @return entries of a specific vertex.
    */
  def from(v: Vertex): List[Entry] = history.filter(_.v == v).toList

  /** *
    * Obtain the entry from the same cloudlet succeeding the informed one.
    * @param entry History entry that the successor is being looked for.
    * @return Optional entry succeeding the informed one.
    */
  def successor(entry: Entry): Option[Entry] = {
    val index = history.indexOf(entry)
    if ((index == -1) || (index == history.length - 1)) None
    else history.drop(index + 1).find(_.cloudlet == entry.cloudlet)
  }

  /**
    * Log a new entry in the history.
    * @param cloudlet Cloudlet to which this entry belongs.
    * @param time Timestamp of the entry.
    * @param v Vertex.
    * @param quantity Quantity of events processed by the vertex.
    */
  def log(cloudlet: String, time: Double, v: Vertex, quantity: Int) = {
    history = history :+ new Entry(cloudlet, time, v, quantity)
  }

  /**
    * Merge the current history with the informed one.
    * @param other The history to be merged with.
    * @return a new history containing entries from both histories. 
    */
  def merge(other: History) =
    new History((this.history ++ other.history).sorted.toList)
  
}
