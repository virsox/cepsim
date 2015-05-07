package ca.uwo.eng.sel.cepsim.event

import ca.uwo.eng.sel.cepsim.event.EventSet

import scala.collection.mutable.Queue


/** EventSetQueue companion objet. */
object EventSetQueue {
  def apply() = new EventSetQueue()
}


/** Queue of event sets */
class EventSetQueue {

  val q = new Queue[EventSet]
  var totalSize = 0.0

  /** Current size of the queue. */
  def size: Double = totalSize

  /** An event set that represent all remaining event sets in queue. */
  def es: EventSet = {
    val result = EventSet.empty()
    q.foreach(result.add(_))
    result
  }

  /**
    * Enqueue event sets.
    * @param eventSets Event sets to be enqueued.
    */
  def enqueue(eventSets: EventSet*) =
    eventSets.foreach((es) => {
      q.enqueue(es)
      totalSize += es.size
    })


  /**
    * Dequeue a number of events from the queue.
    * @param quantity Number of events to be dequeued.
    * @return Event set representing the dequeued events.
    */
  def dequeue(quantity: Double): EventSet = {

    if (quantity > totalSize) throw new NoSuchElementException()

    var remaining = quantity
    val result = EventSet.empty()

    while ((remaining > 0) && (!q.isEmpty)) {
      if (q.head.size <= remaining) {
        val head = q.dequeue()
        remaining -= head.size
        result.add(head)

      } else {
        // in this case, we don't need all events from the event set in
        // the head of the queue
        val extracted = q.head.extract(remaining)
        remaining = 0.0
        result.add(extracted)

        // check for rounding errors
        if (q.head.size < 0.001) q.dequeue
      }
    }
    totalSize -= result.size
    result
  }


}
