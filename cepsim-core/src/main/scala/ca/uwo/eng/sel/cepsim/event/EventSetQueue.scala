package ca.uwo.eng.sel.cepsim.event

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
    * @param es Event set to be enqueued.
    */
  def enqueue(es: EventSet, selectivity: Double = 1.0) = {
    val newSize = es.size * selectivity
    q.enqueue(es.copy(size = newSize))
    totalSize += newSize
  }


  /**
    * Dequeue a number of events from the queue.
    * @param quantity Number of events to be dequeued.
    * @return Event set representing the dequeued events.
    */
  def dequeue(quantity: Double): EventSet = {

    var qty = quantity
    if (qty > totalSize) {

      if (Math.abs(qty - totalSize) < 0.0001) {
        // assuming it is a rounding error
        qty = totalSize

      } else {
        throw new NoSuchElementException("Quantity [" + qty + "] - TotalSize [" + totalSize + "]")
      }

    }

    var remaining = qty
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
        if (q.head.size < 0.0001) q.dequeue
      }
    }
    totalSize -= result.size
    if (totalSize < 0.0001) totalSize = 0

    result
  }


}
