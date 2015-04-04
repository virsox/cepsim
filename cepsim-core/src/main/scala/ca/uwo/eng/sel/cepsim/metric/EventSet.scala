package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.query.EventProducer

/** EventSet companion object. */
object EventSet {

  /** Creates an empty EventSet. */
  def empty(): EventSet = EventSet(0.0, 0.0, 0.0, Map.empty[EventProducer, Double] withDefaultValue(0.0))

  /**
    * Creates an empty EventSet, but initializes the totals map with the informed producers.
    * @param producers EventProducers used in the initialization.
    */
  def withProducers(producers: Set[EventProducer]): EventSet = EventSet(0.0, 0.0, 0.0, producers.map((_, 0.0)).toMap)
}

/**
  * Represents a set of events and associated information that enables metrics to be calculated.
  * This class can be used to represent events that are exchanged between operators, or events that are
  * currently in an operator queue.
  *
  * @param size    Size of the event set.
  * @param ts      Average timestamp of the events. Timestamp represents the moment an event has been last emitted.
  * @param latency Average latency of the events. Latency is measured from the moment en event has been produced.
  * @param totals  Map from event producers to the number of events from these producers that were required to
 *                originate the events currently in the set.
  */
case class EventSet(var size: Double, var ts: Double, var latency: Double, var totals: Map[EventProducer, Double]) {

  /**
    * Add an EventSet to the current one. The new timestamp and latency values are calculated as an weighted average
    * of the current timestamp / latency and the added ones. The new size and total values is simply the sum
    * of the existing and the added ones.
    *
    * @param es EventSet to be added.
    * @param selectivity Selectivity value. It is multiplied with the number of events being added.
    */
  def add(es: EventSet, selectivity: Double = 1.0): Unit = {
    val newQuantity = selectivity * es.size

    if (newQuantity > 0) {
      ts = ((size * ts) + (newQuantity * es.ts)) / (size + newQuantity)
      latency = ((size * latency) + (newQuantity * es.latency)) / (size + newQuantity)
      size    = size + newQuantity
    }
    totals = totals.map((e) => (e._1, e._2 + es.totals.getOrElse(e._1, 0.0))) ++ (es.totals -- totals.keys)
  }

  /**
    * Extracts a number of events from the current set. It returns a new EventSet representing the extracted
    * events, and updates the attributes of the current set.
    *
    * @param quantity Number of events to be extracted.
    * @return EventSet representing the extracted events.
    */
  def extract(quantity: Double): EventSet = {

    // obtains the number of events from each producer that originated the events
    // it is simply calculated proportionally to the total number of events previously on the queue
    val totalFrom = totals.map((e) => (e._1, if (size == 0) 0.0 else (quantity / size) * e._2))

    // update the totals map and size
    totals = totals.map((e) => (e._1, e._2 - totalFrom(e._1)))
    size -= quantity

    EventSet(quantity, ts, latency, totalFrom)
  }

  /** Reset the set attributes.  */
  def reset() = {
    size = 0; ts = 0; latency = 0;
    totals = totals map((e) => (e._1, 0.0))
  }

  override def toString(): String =
    s"(size = $size, timestamp=$ts, latency=$latency, totals = $totals)"

}
