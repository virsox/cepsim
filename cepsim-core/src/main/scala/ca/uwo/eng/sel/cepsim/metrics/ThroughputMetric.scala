package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, InputVertex, EventProducer, Vertex}


/*
  TODO probably a good idea to rename this metric to TotalEventMetric or something and create a new
  ThroughputMetric that depends on this one. Then, we need a way to measure the start time of each
  query (to calculate the throughput since the query execution started) and / or a way to calculate
  the throughput for regular periods of time
*/

/**
 * Throughput metric class. Actually, this metric is still not the throughput but the total number
 * of events that had to be produced in order to generate the events consumed by the vertex v. To obtain
 * the throughput, this calculated value must be divided by the period at which the vertex has been active.
 *
 * @param v EventConsumer of which the metric is calculated.
 * @param time Time of the calculation.
 * @param value Metric value.
 */
case class ThroughputMetric(val v: Vertex, val time: Double, val value: Double) extends Metric


/** ThroughputMetric companion object */
object ThroughputMetric {

  /** Throughput metric identifier - used to register with QueryCloudlet. */
  val ID = "THROUGHPUT_METRIC"

  /**
    * Obtains a calculator for the throughput metric.
    * @param placement Placement of which the metric will be calculated.
    * @return calculator for the throughput metric.
    */
  def calculator(placement: Placement) = new ThroughputCalculator(placement)

  /**
    * Calculator for the Throughput metric.
    * @param placement Placement of which the metric will be calculated.
    */
  class ThroughputCalculator(val placement: Placement) extends MetricCalculator {

    /** Alias for a pair of vertices - used to locate an edge. */
    type EdgeKey = (Vertex, Vertex)

    /**
      * Encapsulates all information of an edge.
      * @param selectivity Edge selectivity.
      * @param size   Current size of the queue attached to this edge.
      * @param totals Total number of events from each producer that originated the events currently
     *                in the queue.
      */
    class EdgeInfo(val selectivity: Double, var size: Double, var totals: Map[EventProducer, Double]) {

      /**
        * Dequeue events from the queue attached to this edge. It updates the queue size and
        * also the totals map.
        * @param eventsNo Number of events to be dequeued.
        * @return Number of events from each producer that originated the dequeued events.
        */
      def dequeue(eventsNo: Double): Map[EventProducer, Double] = {
        val totalTuples = size
        size -= eventsNo

        // obtains the number of events from each producer that originated the events
        // it is simply calculated proportionally to the total number of events previously on the queue
        val totalFrom = totals.map((e) => (e._1, (eventsNo / totalTuples) * e._2))

        // update the totals map
        totals = totals.map((e) => (e._1, e._2 - totalFrom(e._1)))
        totalFrom
      }

      /**
        * Enqueue events to the queue attached to this edge and update the totals map. 
        * @param eventsNo Number of events to be enqueued.
        * @param addToTotal Number of events to be added to totals for each producer.
        */
      def enqueue(eventsNo: Double, addToTotal: Map[EventProducer, Double]): Unit = {
        size  += (eventsNo * selectivity)
        totals = totals map((e) => (e._1, e._2 + addToTotal.getOrElse(e._1, 0.0)))
      }
    }

    /** Edges information. */
    var edges: Map[EdgeKey, EdgeInfo] = Map.empty

    /**
      * For each consumer, it keeps track of the total number of events from each producer
      * that had to be generated in order to originate the events consumed.
      */
    var totalEvents: Map[EventConsumer, Map[EventProducer, Double]] = placement.consumers.map((e) =>
      (e, placement.producers.map((_, 0.0)).toMap)).toMap

    /** Map of metrics calculated for each consumer. */
    var metrics: Map[Vertex, Vector[Metric]] = Map.empty withDefaultValue(Vector.empty[Metric])
    
    /** Number of existing paths from a consumer to each producer. */
    var pathsNo: Map[(EventConsumer, EventProducer), Int] = Map.empty

    // initialize the edges information map
    placement.iterator.foreach((v) => {v match {
      case prod: EventProducer =>
        edges = edges updated((null, prod), new EdgeInfo(1.0, 0.0, Map(prod -> 0.0)))
      case op: InputVertex =>
        op.predecessors.foreach((pred) => {
          edges = edges updated ((pred, op),
            new EdgeInfo(pred.selectivities(op),
                         0.0,
                         placement.producers.map((_, 0.0)).toMap))
        })
    }})

    // initialize the pathsNo map
    placement.consumers.foreach((consumer) => {
      consumer.queries.foreach((query) => {
        query.pathsToProducers(consumer).foreach((path) => {
          val key = (consumer, path.producer)
          pathsNo = pathsNo updated (key, pathsNo.getOrElse(key, 0) + 1)
        })
      })
    })

    /**
     * Gets the calculator identifier.
     * @return calculator identifier.
     */
    override def id: String = ThroughputMetric.ID

    /**
     * Obtains the metric values calculated for a specific vertex.
     * @param v the specified vertex.
     * @return A list of metric values calculated for the vertex.
     */
    override def results(v: Vertex): List[Metric] = metrics(v).toList

    /**
     * Method invoked to update the metric calculation with new processing information.
     * @param event Object encapsulating some important event happened during the simulation.
     */
    override def update(event: Event): Unit = { event match {
      case p: Produced => udateWithProduced(p)
      case p: Processed => updateWithProcessed(p)
      case c: Consumed  => updateWithConsumed(c)

    }}

    /**
     * Consolidates all the metric values that have been calculated for a specific vertex.
     * This implementation simply
     * @param v the specified vertex.
     * @return A single value that consolidates the metric values.
     */
    override def consolidate(v: Vertex): Double =
      results(v).foldLeft(0.0)((acc, metric) => acc + metric.value) / results(v).length

    /**
     * Update the metric calculation with a Produced event.
     * @param produced object encapsulating the event.
     */
    def udateWithProduced(produced: Produced): Unit = {
      val key = (null, produced.v)
      edges(key).enqueue(produced.quantity, Map(produced.v -> produced.quantity))
    }

    /**
     * Update the metric calculation with a Processed event.
     * @param processed object encapsulating the event.
     */
    def updateWithProcessed(processed: Processed): Unit = {
      val v = processed.v

      // update predecessor queue sizes
      val sum = updatePredecessors(v, processed.quantity, processed.processed)

      // update successors queues
      v.successors.foreach((successor) => if (edges.contains((v, successor)))
        edges((v, successor)).enqueue(processed.quantity, sum)
      )
    }


    /**
     * Update the metric calculation with a Consumed event.
     * @param consumed object encapsulating the event.
     */
    def updateWithConsumed(consumed: Consumed): Unit = {

      val consumer = consumed.v
      val sum = updatePredecessors(consumed.v, consumed.quantity, consumed.processed)

      // updates the totalEvents map
      sum.foreach((e) => totalEvents = totalEvents updated (consumer,
        totalEvents(consumer) updated (e._1, totalEvents(consumer)(e._1) + e._2)
      ))

      // calculate the current metric value
      val total = totalEvents(consumer).foldLeft(0.0)((acc, e) => acc + (e._2 / pathsNo(consumer, e._1)))
      metrics = metrics updated (consumer, metrics(consumer) :+ ThroughputMetric(consumer, consumed.at, total))
    }

    /**
     * Auxiliary method used to update the predecessors queues.
     *
     * @param v vertex being processed.
     * @param quantity Number of events generated by the vertex.
     * @param processed Number of events processed from each input queue.
     * @return Number of events from each producer needed to generate the vertex output.
     */
    private def updatePredecessors(v: Vertex, quantity: Double, processed: Map[Vertex, Double]):
      Map[EventProducer, Double] = {

      var sum = Map.empty[EventProducer, Double]
      if (processed.isEmpty) { // EventProducers - does not have predecessors
        sum = edges((null, v)).dequeue(quantity)

      } else {
        processed.foreach((e) => {
          val totalFromProducers = edges((e._1, v)).dequeue(e._2)

          // sum all these totals
          totalFromProducers.foreach((e) => {
            sum = sum updated (e._1, sum.getOrElse(e._1, 0.0) + e._2)
          })
        })
      }
      sum
    }


  } // end of ThroughputCalculator

}


