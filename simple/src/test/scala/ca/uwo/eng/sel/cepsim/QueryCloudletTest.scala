package ca.uwo.eng.sel.cepsim

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

    // Placement inherits many methods from the scala API and it is very hard to mock
    // therefore, we are using a spy here
    val p = new Placement(q, Set(prod, f1, f2, cons), 1)
    val placement = spy(p)
    doReturn(Set(prod)).when(placement).findStartVertices
    doReturn(Iterator(prod, f1, f2, cons)).when(placement).iterator

    val opSchedule = mock[OpScheduleStrategy]
    doReturn(Map(prod -> 100.0, f1 -> 400.0, f2 -> 400.0, cons -> 100.0)).
      when(opSchedule).
      allocate(1000, placement)

  }

  "A QueryCloudlet" should "correctly initialize all operators" in new Fixture {

    val cloudlet = new QueryCloudlet(500 milliseconds, opSchedule)
    cloudlet init(placement)

    verify(prod).init(q)
    verify(f1).init(q)
    verify(f2).init(q)
    verify(cons).init(q)
  }

  it should "correctly run all operators" in new Fixture {
    val cloudlet = new QueryCloudlet(500 milliseconds, opSchedule)
    cloudlet.currentPlacement = placement
    cloudlet.query = q

    doReturn(Set(prod)).when(q).predecessors(f1)
    doReturn(Set(f1)).when(q).predecessors(f2)
    doReturn(Set(f2)).when(q).predecessors(cons)

    doReturn(Map.empty withDefaultValue(0)).when(prod).outputQueues
    doReturn(Map.empty withDefaultValue(0)).when(f1).outputQueues
    doReturn(Map.empty withDefaultValue(0)).when(f2).outputQueues

    // the cloudlet should run all operators
    cloudlet run(1000)

    verify(prod).run(100)
    verify(f1).run(400)
    verify(f2).run(400)
    verify(cons).run(100)
  }

}