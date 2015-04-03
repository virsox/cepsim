package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.query.{WindowedOperator, EventConsumer, EventProducer, Vertex}

/**
 * Created by virso on 15-02-14.
 */
trait Event {
  def v: Vertex
  def quantity: Double
  def at: Double
}

case class Generated (val v: EventProducer, val at: Double, val quantity: Double) extends Event

case class Produced(val v: Vertex, val at: Double, val quantity: Double,
                     val processed: Map[Vertex, Double] = Map.empty) extends Event


case class WindowProduced(val v: WindowedOperator, val at: Double, val quantity: Double, val slot: Int) extends Event

case class WindowAccumulated(val v: WindowedOperator, val at: Double, val slot: Int,
                           val processed: Map[Vertex, Double] = Map.empty) extends Event {
  val quantity: Double = Vertex.sumOfValues(processed)
}

case class Consumed (val v: EventConsumer, val at: Double, val quantity: Double,
                     val processed: Map[Vertex, Double]) extends Event