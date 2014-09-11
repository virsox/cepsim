package ca.uwo.eng.sel.cepsim.integration

import ca.uwo.eng.sel.cepsim.gen.UniformGenerator
import ca.uwo.eng.sel.cepsim.metric.History.Processed
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy
import ca.uwo.eng.sel.cepsim.{QueryCloudlet, Vm}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._


/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class QueryCloudletTest extends FlatSpec
  with Matchers {

  trait Fixture {
    val gen = UniformGenerator(100000, 1.second)

    val prod1 = EventProducer("p1", 1000, gen)
    val f1 = Operator("f1", 4000)
    val f2 = Operator("f2", 4000)
    val cons1 = EventConsumer("c1", 1000)

    val query1 = Query("q1", Set(prod1, f1, f2, cons1), Set((prod1, f1, 1.0), (f1, f2, 1.0), (f2, cons1, 0.1)))

    val vm = Vm("vm1", 1000) // 1 billion instructions per second

  }

  "A QueryCloudlet" should "send events through the operator graph" in new Fixture {


    import ca.uwo.eng.sel.cepsim.metric.History._

    // cloudlet going to use 10 millions instructions (10 ms)
    val cloudlet = QueryCloudlet("c1", Placement(query1, 1), new DefaultOpScheduleStrategy()) //, 0.0)
    val h = cloudlet run (10000000, 0.0, 1000)

    prod1.outputQueues(f1) should be(0)
    f1.outputQueues(f2) should be(0)
    f2.outputQueues(cons1) should be(0)
    cons1.outputQueue should be(100)

    // check if history is being correctly logged
    h.entries should have size (4)
    h.entries should be (List(Processed("c1", 0.0, prod1, 1000), Processed("c1", 1.0, f1, 1000),
      Processed("c1", 5.0, f2, 1000), Processed("c1", 9.0, cons1, 100)))
  }

  it should "accumulate the number of produced events" in new Fixture {
    val cloudlet = QueryCloudlet("c1", Placement(query1, 1), new DefaultOpScheduleStrategy())//, 0.0)
    cloudlet run (10000000, 0.0, 1000)
    cloudlet run (10000000, 10.0, 1000)

    prod1.outputQueues(f1) should be (0)
    f1.outputQueues(f2) should be (0)
    f2.outputQueues(cons1) should be (0)
    cons1.outputQueue should be (200)
  }

  it should "run all queries in the placement" in new Fixture {
    val prod2 = EventProducer("p2", 1000, gen)
    val f3 = Operator("f3", 4000)
    val f4 = Operator("f4", 4000)
    val cons2 = EventConsumer("c2", 1000)
    val query2 = Query("q2", Set(prod2, f3, f4, cons2), Set((prod2, f3, 1.0), (f3, f4, 1.0), (f4, cons2, 0.1)))

    val placement = Placement(query1.vertices ++ query2.vertices, 1)
    val cloudlet = QueryCloudlet("c1", placement, new DefaultOpScheduleStrategy()) //, 0.0)

    val h = cloudlet run (10000000, 0.0, 1000)
    prod1.outputQueues(f1) should be(0)
    f1.outputQueues(f2) should be(0)
    f2.outputQueues(cons1) should be(0)
    cons1.outputQueue should be(50)

    prod2.outputQueues(f3) should be(0)
    f3.outputQueues(f4) should be(0)
    f4.outputQueues(cons2) should be(0)
    cons2.outputQueue should be(50)

    h.entries should have size (8)
    h.entries should contain theSameElementsInOrderAs (List(
      Processed("c1", 0.0, prod1, 500),
      Processed("c1", 0.5, prod2, 500),
      Processed("c1", 1.0, f1, 500),
      Processed("c1", 3.0, f3, 500),
      Processed("c1", 5.0, f2, 500),
      Processed("c1", 7.0, f4, 500),
      Processed("c1", 9.0, cons1, 50),
      Processed("c1", 9.5, cons2, 50)
    ))
  }

}
