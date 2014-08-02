package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.gen.UniformGenerator
import ca.uwo.eng.sel.cepsim.metric.History
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{Query, EventConsumer, Operator, EventProducer}
import ca.uwo.eng.sel.cepsim.sched.EvenOpScheduleStrategy
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._


/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class QueryCloudletIntegrationTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val gen = new UniformGenerator(100000, 1 second)

    val prod1 = new EventProducer("p1", 1000, gen)
    val f1 = Operator("f1", 4000)
    val f2 = Operator("f2", 4000)
    val cons1 = new EventConsumer("c1", 1000)

    var query = Query(Set(prod1, f1, f2, cons1), Set((prod1, f1, 1.0), (f1, f2, 1.0), (f2, cons1, 0.1)))

    val vm = Vm("vm1", 1000) // 1 billion instructions per second
    var cloudlet = QueryCloudlet("c1", Placement(query, vm), new EvenOpScheduleStrategy(), 0.0)

  }

  "A QueryCloudlet" should "send events through the operator graph" in new Fixture {

    //cloudlet init (Placement(query, 1))

    //val h = mock[History]
    //cloudlet.history = h

    import History._

    // cloud going to use 10 millions instructions (10 ms)
    val h = cloudlet run (10)

    prod1.outputQueues(f1) should be(0)
    f1.outputQueues(f2) should be(0)
    f2.outputQueues(cons1) should be(0)
    cons1.outputQueue should be(100)

    // check if history is being correctly logged
    h.entries should have size (4)
    h.entries should be(List(Entry("c1", 0.0, prod1, 1000), Entry("c1", 1.0, f1, 1000),
      Entry("c1", 5.0, f2, 1000), Entry("c1", 9.0, cons1, 100)))
  }

  it should "accumulate the number of produced events" in new Fixture {

    cloudlet run (10)
    cloudlet run (10)

    prod1.outputQueues(f1) should be (0)
    f1.outputQueues(f2) should be (0)
    f2.outputQueues(cons1) should be (0)
    cons1.outputQueue should be (200)
  }


}
