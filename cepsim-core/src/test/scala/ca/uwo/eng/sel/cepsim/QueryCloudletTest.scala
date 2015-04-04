package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.history.History.{Received, Sent}
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
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

    doReturn(Set(prod)).when(f1).predecessors
    doReturn(Set(f1)).when(f2).predecessors
    doReturn(Set(f2)).when(cons).predecessors

    doReturn(Set(f1)).when(prod).successors
    doReturn(Set(f2)).when(f1).successors
    doReturn(Set(cons)).when(f2).successors

    val placement = mock[Placement]
    doReturn(1).when(placement).vmId
    doReturn(Set(prod)).when(placement).producers
    doReturn(Set(prod, f1, f2, cons)).when(placement).vertices

    var opSchedule = mock[OpScheduleStrategy]
    doReturn(Iterator((prod, 100000.0), (f1, 400000.0), (f2, 400000.0), (cons, 100000.0))).
      when(opSchedule).
      allocate(1000000, placement)

  }

  "A QueryCloudlet" should "correctly initialize all operators" in new Fixture {
    val cloudlet = QueryCloudlet.apply("c1", placement, opSchedule)
    cloudlet.init(0.0)

    verify(prod).init(0.0)
    verify(f1).init(0.0)
    verify(f2).init(0.0)
    verify(cons).init(0.0)
  }

  it should "correctly run all operators" in new Fixture {
    val cloudlet = QueryCloudlet.apply("c1", placement, opSchedule) //, 0.0)
    cloudlet.init(0.0)

    doReturn(Map.empty withDefaultValue(0.0)).when(prod).outputQueues
    doReturn(Map.empty withDefaultValue(0.0)).when(f1).outputQueues
    doReturn(Map.empty withDefaultValue(0.0)).when(f2).outputQueues

    // the cloudlet should run all operators
    cloudlet run(1000000, 0.0, 1)

    verify(prod).generate(1000)
    verify(prod).run(100000, 0.0)
    verify(f1).run(400000, 100.0)
    verify(f2).run(400000, 500.0)
    verify(cons).run(100000, 900.0)
  }




  it should "not run operators that are in a different Placement" in new Fixture {
    val cloudlet = QueryCloudlet.apply("c1", placement, opSchedule)

    // create new operators
    val f3 = mock[Operator]
    val cons2 = mock[EventConsumer]


    doReturn(Set(f2)).when(f3).predecessors
    doReturn(Set(f2)).when(cons).predecessors
    doReturn(Set(f3)).when(cons2).predecessors

    doReturn(Set(f3, cons)).when(f2).successors
    doReturn(Set(cons2)).when(f3).successors

    doReturn(Map.empty withDefaultValue(0.0)).when(prod).outputQueues
    doReturn(Map.empty withDefaultValue(0.0)).when(f1).outputQueues
    doReturn(Map(cons -> 100.0, f3 -> 100.0)).when(f2).outputQueues

    // the cloudlet should run all operators
    val history = cloudlet run(1000000, 0.0, 1)

    verify(prod).generate(1000)
    verify(prod).run(100000,   0.0)
    verify(f1  ).run(400000, 100.0)
    verify(f2  ).run(400000, 500.0)
    verify(cons).run(100000, 900.0)

    // these operators shouldn't run
    verify(f3, never()).run(anyLong(), anyLong())
    verify(cons2, never()).run(anyLong(), anyLong())

    val entries = history.from(f2)
    //entries should have size (2)
    //entries should be (List(Processed("c1", 500.0, f2, 0), Sent("c1", 500.0, f2, f3, 100)))
    entries should have size (1)
    entries should be (List(Sent("c1", 500.0, f2, f3, 100)))
  }


  it should "correctly enqueue events received from the network" in new Fixture {
    val cloudlet = QueryCloudlet.apply("c1", placement, opSchedule)
    val history = cloudlet.enqueue(100.0, f1, prod, 1000)

    verify(f1).enqueueIntoInput(prod, 1000)
    val entries = history.from(f1)
    entries should have size (1)
    entries should be (List(Received("c1", 100.0, f1, prod, 1000)))
  }

  // -------------------------------------------------

  it should "correctly split available instructions into iterations" in new Fixture {

    // redefine schedule strategy
    opSchedule = mock[OpScheduleStrategy]

    // this is needed to return a new iterator every time the method is invoked
    doAnswer(new Answer[Iterator[(Vertex, Double)]]() {
      override def answer(inv: InvocationOnMock): Iterator[(Vertex, Double)] =
        Iterator((prod, 50000.0), (f1, 200000.0), (f2, 200000.0), (cons, 50000.0))
    }).when(opSchedule).allocate(500000, placement)

    // 10 iterations
    val cloudlet = QueryCloudlet.apply("c1", placement, opSchedule, 2)
    cloudlet.init(0.0)

    doReturn(Map.empty withDefaultValue(0.0)).when(prod).outputQueues
    doReturn(Map.empty withDefaultValue(0.0)).when(f1).outputQueues
    doReturn(Map.empty withDefaultValue(0.0)).when(f2).outputQueues

    // the cloudlet should run all operators
    cloudlet run(1000000, 0.0, 1)  // 1 million instructions @ 1 MIPS = 1 second

    // two iterations of 500,000 instructions each

    verify(prod, times(2)).generate(500)
    verify(prod).run( 50000,   0.0)
    verify(prod).run( 50000, 500.0)
    verify(f1).run(  200000,  50.0)
    verify(f1).run(  200000, 550.0)
    verify(f2).run(  200000, 250.0)
    verify(f2).run(  200000, 750.0)
    verify(cons).run( 50000, 450.0)
    verify(cons).run( 50000, 950.0)
  }

}