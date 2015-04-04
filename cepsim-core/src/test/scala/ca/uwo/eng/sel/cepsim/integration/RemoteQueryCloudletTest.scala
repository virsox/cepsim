package ca.uwo.eng.sel.cepsim.integration

import ca.uwo.eng.sel.cepsim.QueryCloudlet
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator
import ca.uwo.eng.sel.cepsim.history.History.{Processed, Sent}
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._


/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class RemoteQueryCloudletTest extends FlatSpec
  with Matchers {


  trait Fixture {
    val gen = UniformGenerator(100000)

    val prod1 = EventProducer("p1", 1000, gen)
    val f1 = Operator("f1", 4000)
    val f2 = Operator("f2", 4000)
    val f3 = Operator("f3", 4000)
    val cons1 = EventConsumer("c1", 1000)
    val cons2 = EventConsumer("c2", 1000)

    val query1 = Query("q1", Set(prod1, f1, f2, cons1, cons2),
      Set((prod1, f1, 1.0), (f1, f2, 1.0), (f2, f3, 0.1), (f3, cons2, 0.5), (f2, cons1, 0.1)))
  }

  "A QueryCloudlet" should "run operators in the placement only" in new Fixture {
    val placement = Placement(Set(prod1, f1, f2, cons1), 1)
    val cloudlet = QueryCloudlet("c1", placement, DefaultOpScheduleStrategy.weighted())
    cloudlet.init(0.0)

    val h = cloudlet run (10000000, 0.0, 1000)

    prod1.outputQueues(f1) should be(0)
    f1.outputQueues(f2) should be(0)
    f2.outputQueues(f3) should be(0)
    f2.outputQueues(cons1) should be(0)
    cons1.outputQueue should be(100)

    // check if history is being correctly logged
    h should have size (5)
    h.toList should be (List(
      Processed("c1", 0.0, prod1, 1000),
      Processed("c1", 1.0, f1, 1000),
      Processed("c1", 5.0, f2, 1000),
           Sent("c1", 5.0, f2, f3, 100),
      Processed("c1", 9.0, cons1, 100)))
  }

}
