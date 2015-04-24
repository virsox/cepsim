package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.alloc.{AllocationStrategy, UniformAllocationStrategy}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by virso on 2014-08-15.
 */
@RunWith(classOf[JUnitRunner])
class DynOpScheduleStrategyTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val p1 = mock[EventProducer]
    val p2 = mock[EventProducer]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val c1 = mock[EventConsumer]

    val query1 = mock[Query]

    val placement = mock[Placement]
    doReturn(Set(query1)).when(placement).queries
    doReturn(Set(p1, f1, f2, c1)).when(placement).vertices
    doReturn(Set(p1, f1, f2, c1)).when(placement).vertices(query1)

    // this is needed to return a new iterator every time the method is invoked
    doAnswer(new Answer[Iterator[Vertex]]() {
      override def answer(inv: InvocationOnMock): Iterator[Vertex] = Iterator[Vertex](p1, f1, f2, c1)
    }).when(placement).iterator
  }

  "A DynamicOpScheduleStrategy" should "schedule all operators first" in new Fixture {

    val allocationStrategy = mock[AllocationStrategy]
    doReturn(Map(p1 -> 250.0, f1 -> 250.0, f2 -> 100.0, c1 -> 250.0))
      .when(allocationStrategy).instructionsPerOperator(1000, placement)

    val strategy = DynOpScheduleStrategy(allocationStrategy)
    val ret = strategy.allocate(1000, 0.0, 0.01, placement) // capacity = 0.01 MIPS = 10 instructions per ms

    doReturn(1.0).when(p1).ipe
    doReturn(1.0).when(f1).ipe
    doReturn(1.0).when(f2).ipe
    doReturn(1.0).when(c1).ipe

    doReturn(250.0).when(p1).inputQueue
    ret.next should be (ExecuteAction(p1,  0.0, 25.0, 250))

    doReturn(250.0).when(f1).totalInputEvents
    ret.next should be (ExecuteAction(f1, 25.0, 50.0, 250))

    doReturn(250.0).when(f2).totalInputEvents
    ret.next should be (ExecuteAction(f2, 50.0, 60.0, 100))

    doReturn(200.0).when(c1).totalInputEvents
    ret.next should be (ExecuteAction(c1, 60.0, 80.0, 200))
  }

  it should "reschedule operators if there are remaining instructions" in new Fixture {

    val allocationStrategy = mock[AllocationStrategy]
    doReturn(Map(p1 -> 250.0, f1 -> 250.0, f2 -> 250.0, c1 -> 250.0))
      .when(allocationStrategy).instructionsPerOperator(1000, placement)

    val strategy = new DynOpScheduleStrategy(allocationStrategy)

    val ret = strategy.allocate(1000, 0.0, 0.01, placement)

    doReturn(1.0).when(p1).ipe
    doReturn(1.0).when(f1).ipe
    doReturn(5.0).when(f2).ipe
    doReturn(1.0).when(c1).ipe

    doReturn(150.0).when(p1).inputQueue
    doReturn(150.0).when(f1).totalInputEvents
    doReturn(150.0).when(f2).totalInputEvents
    doReturn( 50.0).when(c1).totalInputEvents

    ret.next should be (ExecuteAction(p1,  0.0, 15.0, 150))
    ret.next should be (ExecuteAction(f1, 15.0, 30.0, 150))
    ret.next should be (ExecuteAction(f2, 30.0, 55.0, 250))
    ret.next should be (ExecuteAction(c1, 55.0, 60.0, 50))

    // ----- round 2
    doReturn(  0.0).when(p1).inputQueue
    doReturn(  0.0).when(f1).totalInputEvents
    doReturn(100.0).when(f2).totalInputEvents
    ret.next should be (ExecuteAction(f2, 60.0, 85.0, 250))

    doReturn(50.0).when(f2).totalInputEvents
    doReturn(50.0).when(c1).totalInputEvents
    ret.next should be (ExecuteAction(c1, 85.0, 90.0, 50))

    // ---- round 3
    ret.next should be (ExecuteAction(f2, 90.0, 100.0, 100))

  }


}
