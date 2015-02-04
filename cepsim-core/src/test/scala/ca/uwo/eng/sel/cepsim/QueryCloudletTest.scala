package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.metric.History.{Received, Sent}
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}


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

    doReturn(Set(prod)).when(q).predecessors(f1)
    doReturn(Set(f1)).when(q).predecessors(f2)
    doReturn(Set(f2)).when(q).predecessors(cons)

    doReturn(Set(f1)).when(q).successors(prod)
    doReturn(Set(f2)).when(q).successors(f1)
    doReturn(Set(cons)).when(q).successors(f2)

    val placement = mock[Placement]
    doReturn(1).when(placement).vmId
    doReturn(Set(prod)).when(placement).producers
    doReturn(Set(prod, f1, f2, cons)).when(placement).vertices

    val opSchedule = mock[OpScheduleStrategy]
    doReturn(Iterator((prod, 100000.0), (f1, 400000.0), (f2, 400000.0), (cons, 100000.0))).
      when(opSchedule).
      allocate(1000000, placement)

  }

  "A QueryCloudlet" should "correctly initialize all operators" in new Fixture {
    val cloudlet = new QueryCloudlet("c1", placement, opSchedule)
    cloudlet.init(0.0)

    verify(prod).init(0.0)
    verify(f1).init(0.0)
    verify(f2).init(0.0)
    verify(cons).init(0.0)
  }

  it should "correctly run all operators" in new Fixture {
    val cloudlet = new QueryCloudlet("c1", placement, opSchedule) //, 0.0)
    cloudlet.init(0.0)

    doReturn(Map.empty withDefaultValue(0.0)).when(prod).outputQueues
    doReturn(Map.empty withDefaultValue(0.0)).when(f1).outputQueues
    doReturn(Map.empty withDefaultValue(0.0)).when(f2).outputQueues

    // the cloudlet should run all operators
    cloudlet run(1000000, 0.0, 1)

    verify(prod).generate()
    verify(prod).run(100000)
    verify(f1).run(400000)
    verify(f2).run(400000)
    verify(cons).run(100000)
  }


  it should "not run operators that are in a different Placement" in new Fixture {
    val cloudlet = new QueryCloudlet("c1", placement, opSchedule)

    // create new operators
    val f3 = mock[Operator]
    val cons2 = mock[EventConsumer]

    doReturn(Set(q)).when(f3).queries
    doReturn(Set(q)).when(cons2).queries

    doReturn(Set(f2)).when(q).predecessors(f3)
    doReturn(Set(f2)).when(q).predecessors(cons)
    doReturn(Set(f3)).when(q).predecessors(cons2)

    doReturn(Set(f3, cons)).when(q).successors(f2)
    doReturn(Set(cons2)).when(q).successors(f3)

    doReturn(Map.empty withDefaultValue(0.0)).when(prod).outputQueues
    doReturn(Map.empty withDefaultValue(0.0)).when(f1).outputQueues
    doReturn(Map(cons -> 100.0, f3 -> 100.0)).when(f2).outputQueues

    // the cloudlet should run all operators
    val history = cloudlet run(1000000, 0.0, 1)

    verify(prod).generate()
    verify(prod).run(100000)
    verify(f1  ).run(400000)
    verify(f2  ).run(400000)
    verify(cons).run(100000)

    // these operators shouldn't run
    verify(f3, never()).run(anyLong())
    verify(cons2, never()).run(anyLong())

    val entries = history.from(f2)
    //entries should have size (2)
    //entries should be (List(Processed("c1", 500.0, f2, 0), Sent("c1", 500.0, f2, f3, 100)))
    entries should have size (1)
    entries should be (List(Sent("c1", 500.0, f2, f3, 100)))
  }


  it should "correctly enqueue events received from the network" in new Fixture {
    val cloudlet = new QueryCloudlet("c1", placement, opSchedule)
    val history = cloudlet.enqueue(100.0, f1, prod, 1000)

    verify(f1).enqueueIntoInput(prod, 1000)
    val entries = history.from(f1)
    entries should have size (1)
    entries should be (List(Received("c1", 100.0, f1, prod, 1000)))
  }


}