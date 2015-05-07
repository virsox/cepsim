package ca.uwo.eng.sel.cepsim.network

import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.query.{InputVertex, OutputVertex}

/**
  * Network interface, used by query cloudlets to send events to remote vertices.
  */
trait NetworkInterface {

  /**
    * Send the events to a remote vertex. Implementation of this method are responsible for calculating the
    * network delay and for invoking the enqueue method in the target cloudlet.
    *
    * @param timestamp Timestamp at which the events have been sent.
    * @param orig Origin vertex.
    * @param dest Destination vertex.
    * @param es Event set that has been sent.
    */
  def sendMessage(timestamp: Double, orig: OutputVertex, dest: InputVertex, es: EventSet)
}
