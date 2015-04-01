package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{InputVertex, EventProducer, Vertex}

/** *
  * Latency metric class.
  * @param v EventConsumer of which the metric is calculated.
  * @param time Time of the calculation.
  * @param quantity Quantity of events to which the calculated metric refers.
  * @param value Calculated latency.
  */
case class LatencyMetric(val v: Vertex, val time: Double, val quantity: Double, val value: Double) extends Metric

/** LatencyMetric companion object. */
object LatencyMetric {

  /** Latency metric identifier - used to register with QueryCloudlet. */
  val ID = "LATENCY_METRIC"

  /**
   * Obtains a calculator for the latency metric.
   * @param placement Placement of which the metric will be calculated.
   * @return calculator for the latency metric.
   */
  def calculator(placement: Placement) = new LatencyMetricCalculator(placement)



  /**
   * Calculator for the latency metric.
   * @param placement Placement of which the metric will be calculated.
   */
  class LatencyMetricCalculator(val placement: Placement) extends MetricCalculator {

    /** Alias for a pair of vertices - used to locate an edge. */
    type EdgeKey = (Vertex, Vertex) // (from, to)

    /**
     * Encapsulates all information of an edge.
     * @param size Current size of the queue attached to this edge.
     * @param timestamp Average timestamp at which currently enqueued events have arrived in the queue (ts_q).
     * @param latency Average latency of currently enqueued events (l_q).
     * @param selectivity Edge selectivity.
     */
    class EdgeInfo(var size: Double, var timestamp: Double, var latency: Double, var selectivity: Double) {

      /**
        * Dequeue events from the queue attached to this edge.
        * @param quantity Events quantity to be dequeued.
        */
      def dequeue(quantity: Double): Unit = {
        size -= quantity
      }

      /**
        * Enqueue events to the queue attached to this edge. Updates the timestamp and latency currently
        * associated with the queues (formulas (4) and (5) from the paper).
        *
        * @param quantity Number of events being enqueued.
        * @param newTimestamp Timestamp at which the enqueing is occurring.
        * @param newLatency Average latency of events being enqueued.
        */
      def enqueue(quantity: Double, newTimestamp: Double, newLatency: Double) = {
        if (quantity > 0) {
          var newQuantity = selectivity * quantity
          timestamp = ((size * timestamp) + (newQuantity * newTimestamp))    / (size + newQuantity)
          latency   = ((size * latency)   + (newQuantity * newLatency)) / (size + newQuantity)
          size     += newQuantity          
        }
      }

      /** Reset queue attributes.  */
      def reset() = {
        size = 0; timestamp = 0; latency = 0;
      }
      
      override def toString(): String =
        s"(size = $size, timestamp=$timestamp, latency=$latency)"

    }

    /** Map of all edges from the placement. */
    var edges: Map[EdgeKey, EdgeInfo] = Map.empty

    /** Map of accumululated events - used for WindowedOperators. */
    var acc: Map[EdgeKey, EdgeInfo] = Map.empty

    /** Map from a vertex to all its outgoing edge keys. */
    var keys: Map[Vertex, Set[EdgeKey]] = Map.empty withDefaultValue(Set.empty)

    /** Map from a event cosumer and calculated metrics.  */
    var metrics: Map[Vertex, Vector[Metric]] = Map.empty withDefaultValue(Vector.empty)


    // initialize attributes
    placement.iterator.foreach((v) => { v match {
      case prod: EventProducer => {
        val key = (null, prod)
        edges = edges updated (key, new EdgeInfo(0.0, 0.0, 0.0, 1.0))
      }
      case op: InputVertex => {
        op.predecessors.foreach((pred) => {
          val key = (pred, op)

          keys = keys updated (pred, keys(pred) + key)
          edges = edges updated (key, new EdgeInfo(0.0, 0.0, 0.0, pred.selectivities(op)))
          acc = acc updated (key, new EdgeInfo(0.0, 0.0, 0.0, 1.0))
        })
      }
    }})

    /**
     * Gets the calculator identifier.
     * @return calculator identifier.
     */
    override def id: String = LatencyMetric.ID

    /**
     * Method invoked to update the metric calculation with new processing information.
     * @param event Object encapsulating some important event happened during the simulation.
     */
    override def update(event: Event) = { event match {
      case p: Produced => updateWithProduced(p)
      case p: Processed => updateWithProcessed(p)
      case c: Consumed => updateWithConsumed(c)
    }}

    /**
      * Update the metric calculation with a Produced event.
      * @param produced object encapsulating the event.
      */
    private def updateWithProduced(produced: Produced) = {
      edges((null, produced.v)) enqueue (produced.quantity, produced.at, 0.0)
    }

    /**
      * Update the metric calculation with a Processed event.
      * @param processed object encapsulating the event.
      */
    private def updateWithProcessed(processed: Processed) = {
      val previous = updatePredecessors(processed, processed.processed)

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
            edges(key) enqueue (processed.quantity, processed.at, accEntry.latency + (processed.at - accEntry.timestamp))
            accEntry reset()

          } else {
            edges(key) enqueue (processed.quantity, processed.at, previous._2 + (processed.at - previous._1))
          }
        }
      })
    }

    /**
      * Update the metric calculation with a Consumed event.
      * @param consumed object encapsulating the event.
      */
    private def updateWithConsumed(consumed: Consumed) = {
      val previous = updatePredecessors(consumed, consumed.processed)
      val consumer = consumed.v

      if (consumed.quantity > 0) {
        metrics = metrics updated (consumer,
          metrics(consumer) :+ LatencyMetric(consumed.v, consumed.at, consumed.quantity,
            previous._2 + (consumed.at - previous._1)))
      }
    }

    /**
     * Auxiliary method used to update the predecessors queues.
     *
     * @param event Object encapsulating a simulation event.
     * @param predQueues Map of predecessors and number of events processed from each one.
     * @return A triplet of the average timestamp, average latency, and total number of events processed
     *         from the predecessors.
     */
    private def updatePredecessors(event: Event, predQueues: Map[Vertex, Double]): (Double, Double, Double) = {
      var timestampSum = 0.0
      var latencySum = 0.0
      var quantitySum = 0.0

      // event producer
      if (predQueues.isEmpty) {
        val key = (null, event.v)

        quantitySum  = event.quantity
        timestampSum = edges(key).timestamp * event.quantity
        latencySum   = 0.0

        edges(key).dequeue(quantitySum)

      } else {
        predQueues.foreach((entry) => {
          val key = (entry._1, event.v)

          quantitySum += entry._2
          timestampSum += (edges(key).timestamp * entry._2)
          latencySum += (edges(key).latency * entry._2)

          edges(key).dequeue(entry._2)
        })
      }
      if (quantitySum == 0) (0.0, 0.0, 0.0)
      else (timestampSum / quantitySum, latencySum / quantitySum, quantitySum)
    }

    /**
     * Obtains the metric values calculated for a specific vertex.
     * @param v the specified vertex.
     * @return A list of metric values calculated for the vertex.
     */
    override def results(v: Vertex): List[Metric] = metrics(v).toList


  }
}


