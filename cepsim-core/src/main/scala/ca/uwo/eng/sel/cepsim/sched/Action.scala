package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.query.Vertex

/**
 * Created by virso on 2015-04-23.
 */
trait Action extends Ordered[Action] {
  /** Vertex that originated the event. */
  def v: Vertex

  /** Start timestamp. */
  def from: Double

  /** End timestamp. */
  def to: Double

  def compare(that: Action): Int = {
    var comp = this.from.compare(that.from)
    if (comp == 0) comp = this.to.compare(that.to)
    if (comp == 0) comp = this.v.compare(that.v)
    comp
  }

}


case class ExecuteAction(val v: Vertex, val from: Double, val to: Double, val instructions: Double) extends Action

case class EnqueueAction(val v: Vertex, val at: Double, val events: Double) extends Action {
  def from: Double = at
  def to: Double = at
}


