package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.metric.{LatencyMetric, ThroughputMetric}
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy

import scala.concurrent.duration._

import ca.uwo.eng.sel.cepsim.gen.UniformGenerator
import ca.uwo.eng.sel.cepsim.query.{Query, EventConsumer, Operator, EventProducer}
import org.scalatest.FlatSpec

/**
 * Created by virso on 2014-08-10.
 */
class QueryVolumeTest extends FlatSpec {

  "A cloudlet" should "run many queries simultaneously" in {
    val vm = Vm("vm1", 1000) // 1 billion instructions per second
    val gen = UniformGenerator(25000, 10 milliseconds)


    val MAX_QUERIES = 20
    var queries = Map.empty[Int, Query]
    for (i <- 1 to MAX_QUERIES) {
      val prod = EventProducer(s"p${i}", 4000, gen)
      val f = Operator(s"f${(i * 2) - 1}", 16000)
      val f2 = Operator(s"f${(i * 2)}", 16000)
      val cons = EventConsumer(s"c${i}", 4000)

      val query = Query(Set(prod, f, f2, cons), Set((prod, f, 1.0), (f, f2, 1.0), (f2, cons, 0.1)))
      queries = queries + (i -> query)
    }

    val MAX_ITERATIONS = 50
    val INTERVAL = 10 // 10 million instructions = 10 milliseconds

    for (i <- 1 to MAX_ITERATIONS) {
      val cloudletName = s"c${i}"
      val cloudlet = QueryCloudlet(cloudletName, Placement.withQueries(queries.values.toSet, vm),
        new DefaultOpScheduleStrategy(), (i - 1) * INTERVAL)

      val h1 = cloudlet run (INTERVAL) // 10 million instructions ~ 10 milliseconds

      val cons = queries(1).consumers.iterator.next
      val t = ThroughputMetric.calculate(queries(1), (i * INTERVAL) milliseconds)
      val l = LatencyMetric.calculate(queries(1), h1, s"c${i}", cons)
      println(f"Iteration [${i}%d]: Size = ${cons.outputQueue}%d, T = ${t}%.2f, L = ${l}%.4f")
    }


  }

}
