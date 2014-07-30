package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.gen.UniformGenerator
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{Query, EventConsumer, Operator, EventProducer}
import ca.uwo.eng.sel.cepsim.sched.EvenOpScheduleStrategy
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._


/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class QueryCloudletIntegrationTest extends FlatSpec
  with Matchers {

  "A QueryCloudlet" should "send events through the operator graph" in {
    val gen = new UniformGenerator(100, 1 second)

    val prod1 = new EventProducer("p1", 100, gen)
    val f1 = Operator("f1", 400)
    val f2 = Operator("f2", 400)
    val cons1 = new EventConsumer("c1", 100)

    var query = Query(Set(prod1, f1, f2, cons1), Set((prod1, f1, 1.0), (f1, f2, 1.0), (f2, cons1, 0.1)))

    var cloudlet = new QueryCloudlet(1 second, new EvenOpScheduleStrategy())

    cloudlet init (Placement(query, 1))
    cloudlet.run(100000)

    prod1.outputQueues(f1) should be (0)
    f1.outputQueues(f2) should be (0)
    f2.outputQueues(cons1) should be (0)
    cons1.outputQueue should be (10)


    //query.addVertex()

  }

}
