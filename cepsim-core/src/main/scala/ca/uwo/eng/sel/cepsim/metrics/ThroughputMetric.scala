package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{InputVertex, EventProducer, Vertex}

/**
 * Created by virso on 15-03-02.
 */
object ThroughputMetric {

  val ID = "THROUGHPUT_METRIC"

  def calculator(placement: Placement) = new ThroughputCalculator(placement)

  class ThroughputCalculator(val placement: Placement) extends MetricCalculator {

    //type QueueKey = (Vertex, Vertex) // (from, to)

    // TODO selectivities refer to successors, size to predecessors - fix
    class MapEntry(val selectivities: Map[Vertex, Double], var size: Map[Vertex, Double], var totals: Map[EventProducer, Double]) {

      def dequeue(dequeued: Map[Vertex, Double]) = {
        size = size map((e) => (e._1, e._2 - dequeued.getOrElse(e._1, 0.0)))
      }

      def enqueue(enqueued: Map[Vertex, Double]) = {
        enqueued foreach((e) => {
          size = size updated (e._1, size(e._1) + e._2 * selectivities(e._1))
        })

      }

      def add(addTosize: Map[Vertex, Double], addToTotal: Map[Vertex, Double]): Unit = {
        size = size map((e) => (e._1, e._2 + addTosize(e._1)))
        totals = totals map((e) => (e._1, e._2 + addToTotal(e._1)))
      }
    }

    var queues: Map[Vertex, MapEntry] = Map.empty
    var metrics: Vector[Metric] = Vector.empty

    placement.iterator.foreach((v) => {v match {
      case prod: EventProducer =>
        queues = queues updated(prod, new MapEntry(Map.empty, Map(prod -> 0.0), Map(prod -> 0.0)))
      case op: InputVertex =>
        queues = queues updated(op, new MapEntry(
          op.predecessors.map((pred) => (pred, pred.selectivities(op))).toMap,
          op.predecessors.map((_, 0.0)).toMap,
          placement.producers.map((_, 0.0)).toMap
        ))

    }})


    override def id: String = ThroughputMetric.ID

    override def results: List[Metric] = ???

    def updateWithProcessed(processed: Processed): Unit = {
      val v = processed.v

      // update predecessor queue sizes
      queues(v).dequeue(processed.processed)

      // update successors queues
      //v.successors.foreach((successor) => if (queues.contains(successor)) queues(successor). )

      // update successors totals

    }

    def updateWithConsumed(consumed: Consumed): Unit = {

    }

    def udateWithProduced(produced: Produced): Unit = {
      val producer = produced.v
      queues(producer).add(Map(producer -> produced.quantity), Map(producer -> produced.quantity))
    }

    override def update(event: Event): Unit = { event match {
      case p: Produced => udateWithProduced(p)
      case p: Processed => updateWithProcessed(p)
      case c: Consumed  => updateWithConsumed(c)

    }}
  }

}
