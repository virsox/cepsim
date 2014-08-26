package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.metric.History
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class QueryCloudletTest extends FlatSpec
	with Matchers
	with MockitoSugar {

  trait Fixture {
    val prod = mock[EventProducer]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val cons = mock[EventConsumer]
    val q = mock[Query]
    doReturn(Set(q)).when(prod).queries
    doReturn(Set(q)).when(f1).queries
    doReturn(Set(q)).when(f2).queries
    doReturn(Set(q)).when(cons).queries

    val vm = mock[Vm]
    doReturn(1.0).when(vm).mips

    val placement = mock[Placement]
    doReturn(1).when(placement).vmId
    doReturn(Set(prod)).when(placement).producers

    val opSchedule = mock[OpScheduleStrategy]
    doReturn(Iterator((prod, 100000.0), (f1, 400000.0), (f2, 400000.0), (cons, 100000.0))).
      when(opSchedule).
      allocate(1000000, placement)

  }


  "A QueryCloudlet" should "correctly run all operators" in new Fixture {
    val cloudlet = new QueryCloudlet("c1", placement, opSchedule) //, 0.0)


    doReturn(Set(prod)).when(q).predecessors(f1)
    doReturn(Set(f1)).when(q).predecessors(f2)
    doReturn(Set(f2)).when(q).predecessors(cons)

    doReturn(Map.empty withDefaultValue(0)).when(prod).outputQueues
    doReturn(Map.empty withDefaultValue(0)).when(f1).outputQueues
    doReturn(Map.empty withDefaultValue(0)).when(f2).outputQueues

    // the cloudlet should run all operators
    cloudlet run(1000000, 0.0, 1)

    verify(prod).generate()
    verify(prod).run(100000)
    verify(f1).run(400000)
    verify(f2).run(400000)
    verify(cons).run(100000)
  }

}