package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.query.{InputVertex, OutputVertex, Vertex}

/**
  * Trait for simulation actions. The temporal relationships between actions are similar to the ones
  * implemented on JBoss Drools.
  */
trait Action extends Ordered[Action] {

  /** Vertex on which the actions is executed. */
  def v: Vertex

  /** Start timestamp. */
  def from: Double

  /** End timestamp. */
  def to: Double

  /**
    * Checks if an action is before another one.
    * @param other Action to be checked.
    * @return true if the action is before the parameter.
    */
  def before(other: Action) =
    (from < other.from) && (to <= other.from)

  /**
   * Checks if an action is after another one.
   * @param other Action to be checked.
   * @return true if the action is after the parameter.
   */
  def after(other: Action) = (from >= other.to)

  /**
   * Checks if an action includes another one.
   * @param other Action to be checked.
   * @return true if the action is after the parameter.
   */
  def include(other: Action) = (from < other.from) && (to > other.to)

  /**
    * Split an action in two at the informed timestamp.
    * @param at The timestamp at which the action should be split.
    * @return A pair of actions resulting from the split.
    */
  def splitAt(at: Double): (Action, Action)


  def compare(that: Action): Int = {
    var comp = this.from.compare(that.from)
    if (comp == 0) comp = this.to.compare(that.to)
    if (comp == 0) comp = this.v.compare(that.v)
    comp
  }
}


/**
  * Vertex execution during simulation.
  *
  * @param v Vertex to be executed.
  * @param from Initial timestamp.
  * @param to Final timestamp.
  * @param instructions Number of instructions to be executed.
  */
case class ExecuteAction(val v: Vertex, val from: Double, val to: Double, val instructions: Double) extends Action {

  def splitAt(at: Double): (Action, Action) = {
    val instructions1 = ((at - from) / (to - from)) * instructions
    val instructions2 = instructions - instructions1
    (ExecuteAction(v, from, at, instructions1), ExecuteAction(v, at, to, instructions2))
  }
}

/**
  * Enqueueing of events (coming from remote vertices) on a specified vertex. Because this is an instantaneous
  * action from the simulator perspective, this action does not have a duration.
  *
  * @param v Vertex on which events should be enqueued.
  * @param fromVertex Vertex from which events are coming.
  * @param at Timestamp of the action.
  * @param es Event set to be enqueued.
  */
case class EnqueueAction(val v: InputVertex, val fromVertex: OutputVertex, val at: Double, val es: EventSet) extends Action {
  def from: Double = at
  def to: Double = at

  def splitAt(at: Double): (Action, Action) = throw new UnsupportedOperationException
}

