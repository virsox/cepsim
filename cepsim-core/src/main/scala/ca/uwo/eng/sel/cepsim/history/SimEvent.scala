package ca.uwo.eng.sel.cepsim.history

import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Vertex, WindowedOperator}

/**
 * Created by virso on 15-02-14.
 */
trait SimEvent {
  def v: Vertex
  def quantity: Double

  def from: Double
  def to: Double
  def at: Double = to
}

case class Generated (val v: EventProducer, val from: Double, val to: Double, val quantity: Double) extends SimEvent {
  //override def at: Double = from
}

case class Produced(val v: Vertex, val from: Double, val to: Double, val quantity: Double,
                     val processed: Map[Vertex, Double] = Map.empty) extends SimEvent


case class WindowProduced(val v: WindowedOperator, val from: Double, val to: Double, val quantity: Double, val slot: Int) extends SimEvent

case class WindowAccumulated(val v: WindowedOperator, val from: Double, val to: Double, val slot: Int,
                           val processed: Map[Vertex, Double] = Map.empty) extends SimEvent {
  val quantity: Double = Vertex.sumOfValues(processed)
}

case class Consumed (val v: EventConsumer, val from: Double, val to: Double, val quantity: Double,
                     val processed: Map[Vertex, Double]) extends SimEvent