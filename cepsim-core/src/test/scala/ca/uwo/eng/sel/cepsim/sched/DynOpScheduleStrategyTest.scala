package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.event.EventSet
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

import scala.collection.immutable.TreeSet

/**
 * Created by virso on 2014-08-15.
 */
@RunWith(classOf[JUnitRunner])
class DynOpScheduleStrategyTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val p1 = mock[EventProducer]("p1")
    val p2 = mock[EventProducer]("p2")
    val f1 = mock[Operator]("f1")
    val f2 = mock[Operator]("f2")
    val c1 = mock[EventConsumer]("c1")
    val ov = mock[Operator]("originVertex")

    when(p1.needsAllocation).thenReturn(true)
    when(f1.needsAllocation).thenReturn(true)
    when(f2.needsAllocation).thenReturn(true)
    when(c1.needsAllocation).thenReturn(true)

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


  trait Fixture1 extends Fixture {
    val allocationStrategy = mock[AllocationStrategy]
    doReturn(Map(p1 -> 250.0, f1 -> 250.0, f2 -> 100.0, c1 -> 250.0))
      .when(allocationStrategy).instructionsPerOperator(1000, placement)

    doReturn(1.0).when(p1).ipe
    doReturn(1.0).when(f1).ipe
    doReturn(1.0).when(f2).ipe
    doReturn(1.0).when(c1).ipe

    var strategy = DynOpScheduleStrategy(allocationStrategy)
  }

  trait Fixture2 extends Fixture {
    val allocationStrategy = mock[AllocationStrategy]
    doReturn(Map(p1 -> 250.0, f1 -> 250.0, f2 -> 250.0, c1 -> 250.0))
      .when(allocationStrategy).instructionsPerOperator(1000, placement)

    var strategy = DynOpScheduleStrategy(allocationStrategy)

    doReturn(1.0).when(p1).ipe
    doReturn(1.0).when(f1).ipe
    doReturn(5.0).when(f2).ipe
    doReturn(1.0).when(c1).ipe
  }


  "A DynamicOpScheduleStrategy" should "schedule all operators first" in new Fixture1 {
    val ret = strategy.allocate(1000, 0.0, 0.01, placement) // capacity = 0.01 MIPS = 10 instructions per ms

    when(p1.instructionsNeeded).thenReturn(250.0)
    when(f1.instructionsNeeded).thenReturn(250.0)
    when(f2.instructionsNeeded).thenReturn(250.0)
    when(c1.instructionsNeeded).thenReturn(200.0)

    ret.next should be (ExecuteAction(p1,  0.0, 25.0, 250))
    ret.next should be (ExecuteAction(f1, 25.0, 50.0, 250))
    ret.next should be (ExecuteAction(f2, 50.0, 60.0, 100))
    ret.next should be (ExecuteAction(c1, 60.0, 80.0, 200))
  }

  it should "consider a single pending action" in new Fixture1 {
    when(p1.instructionsNeeded).thenReturn(0.0)
    when(f1.instructionsNeeded).thenReturn(0.0)
    when(f2.instructionsNeeded).thenReturn(0.0)
    when(c1.instructionsNeeded).thenReturn(0.0)

    when(p1.needsAllocation).thenReturn(false)
    when(f1.needsAllocation).thenReturn(false)
    when(f2.needsAllocation).thenReturn(false)
    when(c1.needsAllocation).thenReturn(false)

    val enqueue1 = EnqueueAction(f1, ov, 0.0, EventSet(10.0, 0.0, 0.0, p1 -> 10.0))
    val pendingActions = TreeSet[Action](enqueue1)
    val ret = strategy.allocate(1000, 0.0, 0.01, placement, pendingActions) // capacity = 0.01 MIPS = 10 instructions per ms

    ret.hasNext should be (true)
    ret.next    should be (enqueue1)
    ret.hasNext should be (false)

  }

  it should "consider a pending action between two actions" in new Fixture1 {
    val enqueue1 = EnqueueAction(c1, ov, 60.0, EventSet(10.0, 50.0, 5.0, p1 -> 10.0))
    val pendingActions = TreeSet[Action](enqueue1)
    val ret = strategy.allocate(1000, 0.0, 0.01, placement, pendingActions) // capacity = 0.01 MIPS = 10 instructions per ms

    when(p1.instructionsNeeded).thenReturn(250.0)
    when(f1.instructionsNeeded).thenReturn(250.0)
    when(f2.instructionsNeeded).thenReturn(500.0)
    when(c1.instructionsNeeded).thenReturn(200.0)

    ret.next should be (ExecuteAction(p1,  0.0, 25.0, 250))
    ret.next should be (ExecuteAction(f1, 25.0, 50.0, 250))
    ret.next should be (ExecuteAction(f2, 50.0, 60.0, 100))
    ret.next should be (enqueue1)
    ret.next should be (ExecuteAction(c1, 60.0, 80.0, 200))
  }

  it should "consider a pending action right in the beginning" in new Fixture1 {

    val enqueue1 = EnqueueAction(f2, ov, 10.0, EventSet(200.0, 0.0, 0.0, p1 -> 200.0))
    val pendingActions = TreeSet[Action](enqueue1)
    val ret = strategy.allocate(1000, 10.0, 0.01, placement, pendingActions) // capacity = 0.01 MIPS = 10 instructions per ms

    when(p1.instructionsNeeded).thenReturn(100.0)
    when(f1.instructionsNeeded).thenReturn(100.0)
    when(f2.instructionsNeeded).thenReturn( 50.0)
    when(c1.instructionsNeeded).thenReturn( 50.0)

    ret.next should be (enqueue1)
    ret.next should be (ExecuteAction(p1, 10.0, 20.0, 100))
    ret.next should be (ExecuteAction(f1, 20.0, 30.0, 100))
    ret.next should be (ExecuteAction(f2, 30.0, 35.0,  50))
    ret.next should be (ExecuteAction(c1, 35.0, 40.0,  50))

    // no vertex has events left
    when(p1.needsAllocation).thenReturn(false)
    when(f1.needsAllocation).thenReturn(false)
    when(f2.needsAllocation).thenReturn(false)
    when(c1.needsAllocation).thenReturn(false)
    ret.hasNext should be (false)
  }

  it should "consider a pending action in the middle of a execute action" in new Fixture1 {

    val enqueue1 = EnqueueAction(f2, ov, 15.0, EventSet(200.0, 10.0, 1.0, p1 -> 200.0))
    val pendingActions = TreeSet[Action](enqueue1)

    val ret = strategy.allocate(1000, 0.0, 0.01, placement, pendingActions) // capacity = 0.01 MIPS = 10 instructions per ms

    when(p1.instructionsNeeded).thenReturn(100.0)
    when(f1.instructionsNeeded).thenReturn(100.0)
    when(f2.instructionsNeeded).thenReturn( 50.0)
    when(c1.instructionsNeeded).thenReturn( 50.0)

    ret.next should be (ExecuteAction(p1, 0.0, 10.0, 100))
    ret.next should be (ExecuteAction(f1, 10.0, 15.0, 50))
    ret.next should be (enqueue1)
    ret.next should be (ExecuteAction(f1, 15.0, 20.0, 50))
    ret.next should be (ExecuteAction(f2, 20.0, 25.0, 50))
    ret.next should be (ExecuteAction(c1, 25.0, 30.0, 50))

    // no vertex has events left
    when(p1.needsAllocation).thenReturn(false)
    when(f1.needsAllocation).thenReturn(false)
    when(f2.needsAllocation).thenReturn(false)
    when(c1.needsAllocation).thenReturn(false)
    ret.hasNext should be (false)
  }


  it should "consider many pending actions" in new Fixture1 {
    val enqueue1 = EnqueueAction(c1, ov,  0.0, EventSet(20.0,  0.0, 0.0, p1 -> 20.0))
    val enqueue2 = EnqueueAction(f2, ov, 40.0, EventSet(30.0, 20.0, 5.0, p1 -> 30.0))
    val enqueue3 = EnqueueAction(c1, ov, 60.0, EventSet(10.0, 50.0, 2.0, p1 -> 10.0))

    val pendingActions = TreeSet[Action](enqueue1, enqueue2, enqueue3)
    val ret = strategy.allocate(1000, 0.0, 0.01, placement, pendingActions)

    when(p1.instructionsNeeded).thenReturn(250.0)
    when(f1.instructionsNeeded).thenReturn(250.0)
    when(f2.instructionsNeeded).thenReturn(500.0)
    when(c1.instructionsNeeded).thenReturn(200.0)

    ret.next should be (enqueue1)
    ret.next should be (ExecuteAction(p1,  0.0, 25.0, 250))
    ret.next should be (ExecuteAction(f1, 25.0, 40.0, 150))
    ret.next should be (enqueue2)
    ret.next should be (ExecuteAction(f1, 40.0, 50.0, 100))
    ret.next should be (ExecuteAction(f2, 50.0, 60.0, 100))
    ret.next should be (enqueue3)
    ret.next should be (ExecuteAction(c1, 60.0, 80.0, 200))
  }

  it should "reschedule operators if there are remaining instructions" in new Fixture2 {

    val ret = strategy.allocate(1000, 0.0, 0.01, placement)


    when(p1.instructionsNeeded).thenReturn(150.0)
    when(f1.instructionsNeeded).thenReturn(150.0)
    when(f2.instructionsNeeded).thenReturn(750.0)
    when(c1.instructionsNeeded).thenReturn( 50.0)

    ret.next should be (ExecuteAction(p1,  0.0, 15.0, 150))
    ret.next should be (ExecuteAction(f1, 15.0, 30.0, 150))
    ret.next should be (ExecuteAction(f2, 30.0, 55.0, 250))
    ret.next should be (ExecuteAction(c1, 55.0, 60.0, 50))

    // ----- round 2
    when(p1.needsAllocation).thenReturn(false)
    when(f1.needsAllocation).thenReturn(false)
    when(f2.instructionsNeeded).thenReturn(500.0)
    ret.next should be (ExecuteAction(f2, 60.0, 85.0, 250))

    when(f2.instructionsNeeded).thenReturn(250.0)
    when(c1.instructionsNeeded).thenReturn( 50.0)
    ret.next should be (ExecuteAction(c1, 85.0, 90.0, 50))

    // ---- round 3
    ret.next should be (ExecuteAction(f2, 90.0, 100.0, 100))

  }

  it should "consider pending actions in the second round of execution" in new Fixture2 {

    val enqueue1 = EnqueueAction(c1, ov, 30.0, EventSet(20.0, 20.0, 1.0, p1 -> 20.0))
    val enqueue2 = EnqueueAction(f2, ov, 70.0, EventSet(30.0, 60.0, 5.0, p1 -> 30.0))
    val enqueue3 = EnqueueAction(c1, ov, 90.0, EventSet(10.0, 70.0, 5.0, p1 -> 10.0))

    val pendingActions = TreeSet[Action](enqueue1, enqueue2, enqueue3)
    val ret = strategy.allocate(1000, 0.0, 0.01, placement, pendingActions)

    when(p1.instructionsNeeded).thenReturn(150.0)
    when(f1.instructionsNeeded).thenReturn(150.0)
    when(f2.instructionsNeeded).thenReturn(750.0)
    when(c1.instructionsNeeded).thenReturn( 50.0)

    ret.hasNext should be (true)
    ret.next should be (ExecuteAction(p1,  0.0, 15.0, 150))
    ret.hasNext should be (true)
    ret.next should be (ExecuteAction(f1, 15.0, 30.0, 150))
    ret.hasNext should be (true)
    ret.next should be (enqueue1)
    ret.hasNext should be (true)
    ret.next should be (ExecuteAction(f2, 30.0, 55.0, 250))
    ret.hasNext should be (true)
    ret.next should be (ExecuteAction(c1, 55.0, 60.0, 50))

    // ----- round 2
    when(p1.needsAllocation).thenReturn(false)
    when(f1.needsAllocation).thenReturn(false)
    when(f2.instructionsNeeded).thenReturn(500.0)

    ret.hasNext should be (true)
    ret.next should be (ExecuteAction(f2, 60.0, 70.0, 100))
    ret.hasNext should be (true)
    ret.next should be (enqueue2)
    ret.hasNext should be (true)
    ret.next should be (ExecuteAction(f2, 70.0, 85.0, 150))

    when(f2.instructionsNeeded).thenReturn(250.0)
    when(c1.instructionsNeeded).thenReturn( 50.0)
    ret.hasNext should be (true)
    ret.next should be (ExecuteAction(c1, 85.0, 90.0, 50))
    ret.hasNext should be (true)
    ret.next should be (enqueue3)

    // ---- round 3
    when(c1.needsAllocation).thenReturn(false)
    ret.hasNext should be (true)
    ret.next should be (ExecuteAction(f2, 90.0, 100.0, 100))

    when(f2.needsAllocation).thenReturn(false)
    ret.hasNext should be (false)
  }

}
