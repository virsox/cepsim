package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.query.Vertex

/** Companion object. */
object History {
  def apply() = new History()

  /***
    * An entry in the cloudlet execution history
    * @param cloudlet Cloudlet to which this entry belongs.
    * @param time Timestamp of the entry.
    * @param v Vertex.
    * @param quantity Quantity of events processed by the vertex.
    */
  case class Entry(cloudlet: String, time: Double, v: Vertex, quantity: Int)
}

/** Represent a cloudlet execution history. */
class History() {

  import ca.uwo.eng.sel.cepsim.metric.History._

  /** vector of entries */
  var history: Vector[Entry] = Vector.empty

  /**
    * Obtain all entries as a list.
    * @return all entries as a list.
    */
  def entries(): List[Entry] = history.toList

  /**
   * Obtain the entry from a specific cloudlet of a specific vertex.
   * @param cloudlet Name of cloudlet.
   * @param v Vertex of the entries.
   * @return Optional entry of the specific vertex.
   */
  def from(cloudlet: String, v: Vertex): Option[Entry] = {
    val result = history.filter((e) => (e.cloudlet == cloudlet) && (e.v == v)).toList
    result match {
      case head :: tail => Some(head)
      case Nil => None
    }

  }

  /**
    * Obtain entries of a specific vertex.
    * @param v Vertex of the entries.
    * @return entries of a specific vertex.
    */
  def from(v: Vertex): List[Entry] = {
    history.filter(_.v == v).toList
  }

  /** *
    * Obtain the entry succeeding the informed one.
    * @param entry History entry that the successor is being looked for.
    * @return Optional entry succeeding the informed one.
    */
  def successor(entry: Entry): Option[Entry] = {
    val index = history.indexOf(entry)
    if ((index == -1) || (index == history.length - 1)) None
    else Some(history(index + 1))
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

}
