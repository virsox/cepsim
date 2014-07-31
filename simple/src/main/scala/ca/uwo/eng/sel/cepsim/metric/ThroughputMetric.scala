package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.query.{EventProducer, EventConsumer, Query}

import scala.concurrent.duration.Duration


/**
 * Created by virso on 2014-07-30.
 */
object ThroughputMetric {


  def calculate(q: Query, totalTime: Duration): Double = {
    val producers = q.producers

    // sum of all producers average
    val totalProducersWeight = producers.foldLeft(0.0)((acc, producer) => acc + producer.generator.average)

    // proportion of each producer to the sum
    val producersWeight = producers.map((p) => (p, p.generator.average / totalProducersWeight)).
      toMap[EventProducer, Double]

    // sum of all generated events
    // for each consumer, it is deducted the total number of events that needs to be generated
    val totalEvents = q.consumers.foldLeft(0.0)((acc, consumer) => acc + totalEvent(q, producersWeight, consumer))

    // divide by the time
    totalEvents / totalTime.toSeconds
  }

  private def totalEvent(q: Query, producerWeights: Map[EventProducer, Double], c: EventConsumer): Double = {

    val paths = q.pathsToProducers(c)
    val pathWeights = paths.map{(p) =>
      p.edges.foldLeft(1.0)((acc, edge) => acc * edge.selectivity) * producerWeights(p.producer)
    }

    val totalPathWeights = pathWeights.foldLeft(0.0)((acc, weight) => acc + weight)
    c.outputQueue / totalPathWeights
  }



}
