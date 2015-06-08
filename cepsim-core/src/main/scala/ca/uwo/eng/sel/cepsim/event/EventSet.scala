package ca.uwo.eng.sel.cepsim.event

import java.util.{Map => JavaMap}

import ca.uwo.eng.sel.cepsim.query.EventProducer

import scala.collection.JavaConversions.mapAsScalaMap


/** EventSet companion object. */
object EventSet {

  /** Creates an empty EventSet. */
  def empty(): EventSet = new EventSet(0.0, 0.0, 0.0, Map.empty[EventProducer, Double] withDefaultValue(0.0))

  /**
    * Creates an empty EventSet, but initializes the totals map with the informed producers.
    * @param producers EventProducers used in the initialization.
    */
  def withProducers(producers: Set[EventProducer]): EventSet = new EventSet(0.0, 0.0, 0.0, producers.map((_, 0.0)).toMap)

  /**
    * Creates an EventSet with the informed parameters. Facilitates the instantiation of an EventSet by accepting
    * a variable number of tuples instead of a map.
    */
  def apply(size: Double, ts: Double, latency: Double, totals: (EventProducer, Double)*) =
    new EventSet(size, ts, latency, totals.toMap)


  /**
    * Auxiliary method that returns the total number of events present in a collection of event sets.
    * @param eventSets collection of event sets.
    * @return Total number of events present in the collection.
    */
  def totalSize(eventSets: Iterable[EventSet]): Double =
    eventSets.foldLeft(0.0)((acc, elem) => acc + elem.size)

  /**
    * Auxiliary method that returns a new event set containing the sum of all event sets from a collection.
    * @param eventSets collection of events.
    * @return Event set with the sum of all events sets.
    */
  def addAll(eventSets: Iterable[EventSet]): EventSet = {
    val sum = empty()
    eventSets.foreach((es) => sum.add(es))
    sum
  }

}

/**
  * Represents a set of events and associated information that enables metrics to be calculated.
  * This class can be used to represent events that are exchanged between operators, or events that are
  * accumulated (in an event queue or in some temporary buffer).
  *
  * @param size    Size of the event set.
  * @param ts      Average timestamp of the events. Timestamp represents the moment an event has been last emitted.
  * @param latency Average latency of the events. Latency is measured from the moment en event has been generated.
  * @param totals  Map from event producers to the number of events from these producers that were required to
 *                originate the events currently in the set.
  */
case class EventSet(var size: Double, var ts: Double, var latency: Double, var totals: Map[EventProducer, Double]) {

  // constructor for java usage
  def this(size: Double, ts: Double, latency: Double, totals: JavaMap[EventProducer, Double]) =
    this(size, ts, latency, mapAsScalaMap(totals).toMap)

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
    val newSize = size + newQuantity

    if (newQuantity > 0) {
      ts = ((size * ts) + (newQuantity * es.ts)) / newSize
      latency = ((size * latency) + (newQuantity * es.latency)) / newSize
      size    = newSize
    }
    totals = totals ++ es.totals.map{ case (k, v) => k -> (v + totals.getOrElse(k, 0.0)) }
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

  /**
    * Splits the event set into two sets. The first set contains a percentage of the events, and the second
    * contains all remaining events. The timestamp and latency of the resulting sets are not modified, but the
    * totals map is also split according to the informed percentage.
    *
    * @param percentage Percentage of events that should be in the first set.
    * @return A pair of new event sets containing the result of the split.
    */
  def split(percentage: Double): (EventSet, EventSet) = {
    val first = EventSet(percentage * size, ts, latency, totals.map((elem) => elem._1 -> elem._2 * percentage))
    val second = EventSet(size - first.size, ts, latency, totals.map((elem) => elem._1 -> (elem._2 - first.totals(elem._1))))
    (first, second)
  }

  /**
    * Update the timestamp of the event set.
    * @param at New timestamp.
    */
  def updateTimestamp(at: Double) = {
    latency = (at - ts) + latency
    ts = at
  }

  /** Reset the set attributes.  */
  def reset() = {
    size = 0; ts = 0; latency = 0;
    totals = totals map((e) => (e._1, 0.0))
  }

  override def toString(): String =
    s"(size = $size, timestamp=$ts, latency=$latency, totals = $totals)"

}
