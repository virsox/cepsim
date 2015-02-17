package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.query.Vertex

/**
 * Created by virso on 15-02-14.
 */
trait Event {
  def v: Vertex
  def quantity: Double
  def at: Double
}

case class Produced (val v: Vertex, val at: Double, val quantity: Double) extends Event

case class Processed(val v: Vertex, val at: Double, val quantity: Double,
                     val processed: Map[Vertex, Double] = Map.empty) extends Event

case class Consumed (val v: Vertex, val at: Double, val quantity: Double,
                     val processed: Map[Vertex, Double]) extends Event