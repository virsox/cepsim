package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{InputVertex, EventProducer, Vertex}


object LatencyMetric {

  val ID = "LATENCY_METRIC"

  def calculator(placement: Placement) = new LatencyMetricCalculator(placement)

  // ------------------------------------------------------------------


  class LatencyMetricCalculator(val placement: Placement) extends MetricCalculator {
    type QueueKey = (Vertex, Vertex) // (from, to)

    class QueueEntry(var size: Double, var timestamp: Double, var latency: Double, var selectivity: Double) {

      def dequeue(quantity: Double): Unit = {
        size -= quantity
      }

      def enqueue(quantity: Double, newTime: Double, newLatency: Double) = {
        var newQuantity = selectivity * quantity
        timestamp = ((size * timestamp) + (newQuantity * newTime))    / (size + newQuantity)
        latency   = ((size * latency)   + (newQuantity * newLatency)) / (size + newQuantity)
        size     += newQuantity
      }

      def reset() = {
        size = 0; timestamp = 0; latency = 0;
      }

    }

    var queues: Map[QueueKey, QueueEntry] = Map.empty
    var acc: Map[QueueKey, QueueEntry] = Map.empty

    var keys: Map[Vertex, Set[QueueKey]] = Map.empty withDefaultValue(Set.empty)
    var metrics: Vector[Metric] = Vector.empty


    placement.iterator.foreach((v) => { v match {
      case prod: EventProducer => {
        val key = (null, prod)
        queues = queues updated (key, new QueueEntry(0.0, 0.0, 0.0, 1.0))
      }
      case op: InputVertex => {
        op.predecessors.foreach((pred) => {
          val key = (pred, op)

          keys = keys updated (pred, keys(pred) + key)
          queues = queues updated (key, new QueueEntry(0.0, 0.0, 0.0, pred.selectivities(op)))
          acc = acc updated (key, new QueueEntry(0.0, 0.0, 0.0, 1.0))
        })
      }
    }})

    override def id: String = LatencyMetric.ID

    override def update(event: Event) = { event match {
      case p: Produced => updateWithProduced(p)
      case p: Processed => updateWithProcessed(p)
      case c: Consumed => updateWithConsumed(c)
      //case _ => throw new IllegalArgumentException()
    }}


    private def updateWithProduced(produced: Produced) = {
      //val avgTime = ((produced.at - produced.from) / 2) + produced.from
      queues((null, produced.v)) enqueue (produced.quantity, produced.at, 0.0)
    }


    private def updatePredecessors(event: Event, predQueues: Map[Vertex, Double]): (Double, Double, Double) = {
      var timestampSum = 0.0
      var latencySum = 0.0
      var quantitySum = 0.0

      // event producer
      if (predQueues.isEmpty) {
        val key = (null, event.v)

        quantitySum = event.quantity
        timestampSum = queues(key).timestamp * event.quantity
        latencySum = 0.0

        queues(key).dequeue(quantitySum)

      } else {
        predQueues.foreach((entry) => {
          val key = (entry._1, event.v)

          quantitySum += entry._2
          timestampSum += (queues(key).timestamp * entry._2)
          latencySum += (queues(key).latency * entry._2)

          queues(key).dequeue(entry._2)
        })
      }
      (timestampSum / quantitySum, latencySum / quantitySum, quantitySum)
    }


    private def updateWithProcessed(processed: Processed) = {
      val previous = updatePredecessors(processed, processed.queues)

      keys(processed.v).foreach((key) => {

        // Window operator - accumulating
        if (processed.quantity == 0) {
          acc(key) enqueue (previous._3, processed.at, previous._2 + (processed.at - previous._1))

        } else {

          if (acc(key).size > 0) {

            val accEntry = acc(key)

            // still accumulates for this iteration
            accEntry enqueue (previous._3, processed.at, previous._2 + (processed.at - previous._1))

            // enqueue
            queues(key) enqueue (processed.quantity, processed.at, accEntry.latency + (processed.at - accEntry.timestamp))
            accEntry reset()
          } else {
            queues(key) enqueue (processed.quantity, processed.at, previous._2 + (processed.at - previous._1))
          }
        }
      })


    }

    private def updateWithConsumed(consumed: Consumed) = {
      val previous = updatePredecessors(consumed, consumed.queues)

      if (consumed.quantity > 0) {
        metrics = metrics :+ LatencyMetric(consumed.v, consumed.at, consumed.quantity,
          previous._2 + (consumed.at - previous._1))
      }
    }

    override def results: List[Metric] = metrics.toList


  }
}



/**
 * Created by virso on 15-02-14.
 */
case class LatencyMetric(val v: Vertex, val time: Double, val quantity: Double, val value: Double) extends Metric