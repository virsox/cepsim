package ca.uwo.eng.sel.cepsim.metric

import java.io.{PrintWriter, FileWriter, BufferedWriter, FileOutputStream}

import ca.uwo.eng.sel.cepsim.metric.History.Entry
import ca.uwo.eng.sel.cepsim.metric.History.Processed
import ca.uwo.eng.sel.cepsim.query.{EventProducer, EventConsumer, Query}

import scala.collection.mutable
import scala.collection.mutable.LinkedList
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

/** Calculates the latency metric */
object LatencyMetric extends Metric {

  
  /**
    * Calculates the average latency among all consumers from the query.
    * @param query Query from which the latency is being calculated.
    * @param history history Execution history of this query.
    */
  def calculate(query: Query, history: History[Entry]): Double = {
    val sum = query.consumers.foldLeft(0.0)((acc, consumer) => {
      acc + calculate(query, history, consumer)
    })
    sum / query.consumers.size
  }
  
    /**
    * Calculates the average latency of a single consumer from a query.
    * @param query Query to which the consumer belongs.
    * @param history history Execution history of the query.
    * @param consumer Latency from which the latency is being calculated.
    */
  def calculate(query: Query, history: History[Entry], consumer: EventConsumer): Double = {

    // all consumer entries
    val entries = history.processedEntriesFrom(consumer).filter(_.quantity > 0)

    val producerEntriesSet = query.producers.map((p) => {
      (p, mutable.LinkedList(history.processedEntriesFrom(p):_*))
    })

    //val out = new PrintWriter(new FileWriter("/Users/virso/tmp/latencies.txt"))
    val producerEntries = HashMap(producerEntriesSet.toList:_*)
    val sum = entries.foldLeft(0.0)((acc, entry) => {
      val perEntry = internalCalculate(query, producerEntries, consumer, entry)
      //out.println(entry.time + ", " + entry.quantity + ", " + perEntry)
      acc + perEntry
    })
    //out.close();
    
    sum / entries.size
  }
  
  /**
    * Calculate the latency of the events consumed by a consumer at a specific time.
    *
    * @param query Query to which the consumer belongs.
    * @param history History that contains the query execution.
    * @param consumer Consumer of which the latency is calculated.
    * @param time Simulation time when the latency is calculated. 
    * @return Latency (in ms).
    */
  def calculate(query: Query, history: History[Entry], consumer: EventConsumer, time: Double): Double = {

    var historyTmp = history

    val producerEntriesSet = query.producers.map((p) => {
      (p, mutable.LinkedList(history.processedEntriesFrom(p):_*))
    })
    val producerEntries = HashMap(producerEntriesSet.toList:_*)

    var entries = history.processedEntriesFrom(consumer)
    val (removed, remaining) = entries.span(_.time < time)

    removed.foreach((entry) => {
      internalCalculate(query, producerEntries, consumer, entry)
    })
    internalCalculate(query, producerEntries, consumer, remaining.head)
  }


//  /**
//   * Method used internally to calculate latency. It calculates the latency for a specific consumer
//   * at a specific time, but also return a new History with the entries used in the calculation removed.
//   *
//   * @param query Query to which the consumer belongs.
//   * @param history Query execution history.
//   * @param consumer Consumer of which the latency is calculated.
//   * @param time Simulation time when the latency is calculated.
//   * @return A pair containing the calculated latency and the new History.
//   */
//  private def internalCalculate(query: Query, history: History[Entry], consumer: EventConsumer,
//                                time: Double): (Double, History[Entry]) = {
//    val entry = history.processedEntriesFrom(consumer, time)
//    entry match {
//      case Some(consumerEntry) => internalCalculate(query, history, consumer, consumerEntry)
//      case None => throw new IllegalArgumentException()
//    }
  //}


  private def internalCalculate(query: Query,
                                producerEntries: Map[EventProducer, LinkedList[Processed]],
                                consumer: EventConsumer,
                                consumerEntry: Processed): Double = {//, History[Entry]) = {

    //var historyTmp = history

    /**
     * Estimate the time in the simulation timeline when the producer started producing the
     * informed number of events.
     * @param producer Producer that is being tracked.
     * @param events Total number of events produced.
     * @param consumerTime The consumer entry time.
     * @return Minimum time (before the cloudlet execution) when the events started to be produced.
     */
    def minimumTime(producer: EventProducer, events: Double, consumerTime: Double): Double = {

      if (events == 0)
        return Double.NegativeInfinity

      // select the first entries needed to generate the output
      var totalEvents = events

      var (neededEntries, remaining) = producerEntries(producer).span((entry) => {
        if (entry.time < consumerTime) {
          if (totalEvents <= 0) false
          else {
            totalEvents -= entry.quantity; true
          }
        } else false
      })

      val minimumTime = neededEntries.head.time
      //historyTmp = historyTmp.remove(neededEntries:_*)

      // fix history - in case the newest entry from neededEntries hasn't been entirely processed
      if (totalEvents < 0) {
        val newest = neededEntries.last
        val remainingQuantity = -totalEvents // totalEvents is already negative
        remaining = Processed(newest.cloudlet, newest.time, newest.v, remainingQuantity) +: remaining
      }
      producerEntries.update(producer, remaining)

      minimumTime
    }

    val eventsPerProducerResult = this.eventsPerProducer(query, consumer, consumerEntry.quantity.toInt)
    val minimumPerProducer = eventsPerProducerResult.map((entry) =>
      (entry._1, minimumTime(entry._1, entry._2, consumerEntry.time))
    )
    consumerEntry.time - minimumPerProducer.values.min//, historyTmp)

  }

}
