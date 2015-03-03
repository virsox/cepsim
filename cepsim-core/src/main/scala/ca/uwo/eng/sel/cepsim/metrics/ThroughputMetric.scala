package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, InputVertex, EventProducer, Vertex}

/**
 * Created by virso on 15-03-02.
 */
object ThroughputMetric {

  val ID = "THROUGHPUT_METRIC"

  //def apply(v: Vertex, time: Double, value: Double) = new ThroughputMetric(v, time, value)
  def calculator(placement: Placement) = new ThroughputCalculator(placement)

  class ThroughputCalculator(val placement: Placement) extends MetricCalculator {

    type QueueKey = (Vertex, Vertex) // (from, to)

    class MapEntry(val selectivity: Double, var size: Double, var totals: Map[EventProducer, Double]) {
      def dequeue(dequeued: Double): Map[EventProducer, Double] = {

        val totalTuples = size
        size -= dequeued

        val totalFrom = totals.map((e) => (e._1, (dequeued / totalTuples) * e._2))
        totals = totals.map((e) => (e._1, e._2 - totalFrom(e._1)))

        totalFrom

      }


      //      def dequeue(dequeued: Map[Vertex, Double]) = {
//        size = size map((e) => (e._1, e._2 - dequeued.getOrElse(e._1, 0.0)))
//      }
//
//      def enqueue(enqueued: Map[Vertex, Double]) = {
//        enqueued foreach((e) => {
//          size = size updated (e._1, size(e._1) + e._2 * selectivities(e._1))
//        })
//
//      }
//
      def enqueue(addToSize: Double, addToTotal: Map[EventProducer, Double]): Unit = {
        size  += (addToSize * selectivity)
        totals = totals map((e) => (e._1, e._2 + addToTotal.getOrElse(e._1, 0.0)))
      }
    }

    var queues: Map[QueueKey, MapEntry] = Map.empty

    var totals: Map[EventConsumer, Map[EventProducer, Double]] = placement.consumers.map((e) =>
      (e, placement.producers.map((_, 0.0)).toMap)).toMap

    var metrics: Vector[Metric] = Vector.empty
    var paths: Map[(EventConsumer, EventProducer), Int] = Map.empty

    placement.iterator.foreach((v) => {v match {
      case prod: EventProducer =>
        queues = queues updated((null, prod), new MapEntry(1.0, 0.0, Map(prod -> 0.0)))
      case op: InputVertex =>
        op.predecessors.foreach((pred) => {
          queues = queues updated ((pred, op),
            new MapEntry(pred.selectivities(op),
                         0.0,
                         placement.producers.map((_, 0.0)).toMap))
        })
    }})

    placement.consumers.foreach((consumer) => {
      consumer.queries.foreach((query) => {
        query.pathsToProducers(consumer).foreach((path) => {
          val key = (consumer, path.producer)
          paths = paths updated (key, paths.getOrElse(key, 0) + 1)
        })
      })
    })


    override def id: String = ThroughputMetric.ID

    override def results: List[Metric] = metrics.toList


    private def updatePredecessors(v: Vertex, quantity: Double, processed: Map[Vertex, Double]): Map[EventProducer, Double] = {
      var sum = Map.empty[EventProducer, Double]

      if (processed.isEmpty) { // EventProducer
        sum = queues((null, v)).dequeue(quantity)

      } else {
        processed.foreach((e) => {
          val totalFromProducers = queues((e._1, v)).dequeue(e._2)

          // sum all these totals
          totalFromProducers.foreach((e) => {
            sum = sum updated (e._1, sum.getOrElse(e._1, 0.0) + e._2)
          })
        })
      }


      sum
    }

    def updateWithProcessed(processed: Processed): Unit = {
      val v = processed.v

      // update predecessor queue sizes
      val sum = updatePredecessors(v, processed.quantity, processed.processed)

      // update successors queues
      v.successors.foreach((successor) => if (queues.contains((v, successor)))
        queues((v, successor)).enqueue(processed.quantity, sum)
      )

    }

    def updateWithConsumed(consumed: Consumed): Unit = {

      val consumer = consumed.v
      val sum = updatePredecessors(consumed.v, consumed.quantity, consumed.processed)

      sum.foreach((e) => totals = totals updated (consumer,
        totals(consumer) updated (e._1, totals(consumer)(e._1) + e._2)
      ))

      val total = totals(consumer).foldLeft(0.0)((acc, e) => acc + (e._2 / paths(consumer, e._1)))
      metrics = metrics :+ ThroughputMetric(consumer, consumed.at, total)
    }

    def udateWithProduced(produced: Produced): Unit = {
      val key = (null, produced.v)
      queues(key).enqueue(produced.quantity, Map(produced.v -> produced.quantity))
    }

    override def update(event: Event): Unit = { event match {
      case p: Produced => udateWithProduced(p)
      case p: Processed => updateWithProcessed(p)
      case c: Consumed  => updateWithConsumed(c)

    }}
  }

}

case class ThroughputMetric(val v: Vertex, val time: Double, val value: Double) extends Metric
