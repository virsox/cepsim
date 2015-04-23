package ca.uwo.eng.sel.cepsim.integration

import ca.uwo.eng.sel.cepsim.gen.UniformGenerator
import ca.uwo.eng.sel.cepsim.metric.{ThroughputMetric, LatencyMetric}
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy
import ca.uwo.eng.sel.cepsim.{QueryCloudlet, Vm}
import org.scalatest.FlatSpec

import scala.concurrent.duration._

/**
 * Created by virso on 2014-08-10.
 */
class QueryVolumeTest extends FlatSpec {

  "A cloudlet" should "run many queries simultaneously" in {
    val vm = Vm("vm1", 1000) // 1 billion instructions per second
    val gen = UniformGenerator(25000)


    val MAX_QUERIES = 20
    var queries = Map.empty[Int, Query]
    for (i <- 1 to MAX_QUERIES) {
      val prod = EventProducer(s"p${i}", 4000, gen)
      val f = Operator(s"f${(i * 2) - 1}", 16000)
      val f2 = Operator(s"f${(i * 2)}", 16000)
      val cons = EventConsumer(s"c${i}", 4000)

      val query = Query(s"q${i}", Set(prod, f, f2, cons), Set((prod, f, 1.0), (f, f2, 1.0), (f2, cons, 0.1)))
      queries = queries + (i -> query)
    }

    val MAX_ITERATIONS = 50
    val INTERVAL = 10 // 10 million instructions = 10 milliseconds

    for (i <- 1 to MAX_ITERATIONS) {
      val cloudletName = s"c${i}"
      val cloudlet = QueryCloudlet(cloudletName, Placement.withQueries(queries.values.toSet, 1),
        DefaultOpScheduleStrategy.weighted())//, (i - 1) * INTERVAL)
      cloudlet.init((i - 1) * INTERVAL)

      val h1 = cloudlet run (INTERVAL * 1000000, (i - 1) * INTERVAL, 10) // 10 million instructions ~ 10 milliseconds

      val cons = queries(1).consumers.iterator.next

      val t = cloudlet.metric(ThroughputMetric.ID, cons)
      val l = cloudlet.metric(LatencyMetric.ID, cons)
      println(f"Iteration [${i}%d]: Size = ${cons.outputQueue}%.4f, T = ${t}%.2f, L = ${l}%.4f")
    }


  }

}
