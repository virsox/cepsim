package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.query.{Vertex, Query}

/**
 * Created by virso on 2014-08-01.
 */

object History {
  def apply() = new History()

  case class Entry(cloudlet: String, time: Double, v: Vertex, quantity: Int)
}

class History() {

  import History._

  var history: Vector[Entry] = Vector.empty

  def entries(): List[Entry] = history.toList

  def from(v: Vertex): List[Entry] = {
    history.filter(_.v == v).toList
  }

  def log(cloudlet: String, time: Double, v: Vertex, quantity: Int) = {
    history = history :+ new Entry(cloudlet, time, v, quantity)
  }

}
