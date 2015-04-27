package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import ca.uwo.eng.sel.cepsim.sched.alloc.AllocationStrategy
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.TreeSet

/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class DefaultOpScheduleStrategyTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val p1 = mock[EventProducer]
    val f1 = mock[Operator]
    val c1 = mock[EventConsumer]

    doReturn(10.0).when(p1).ipe
    doReturn(80.0).when(f1).ipe
    doReturn(10.0).when(c1).ipe

    val query1 = mock[Query]


    val placement = mock[Placement]
    doReturn(Iterator(p1, f1, c1)).when(placement).iterator

    val allocStrategy = mock[AllocationStrategy]
    doReturn(Map(p1 -> 200000.0, f1 -> 600000.0, c1 -> 200000.0)).when(allocStrategy).instructionsPerOperator(1000000.0, placement)

    val a1 = ExecuteAction(p1,   0.0,  200.0, 200000.0)
    val a2 = ExecuteAction(f1, 200.0,  800.0, 600000.0)
    val a3 = ExecuteAction(c1, 800.0, 1000.0, 200000.0)
  }


  trait Fixture2 {
    val p2 = mock[EventProducer]
    val f2 = mock[Operator]
    val c2 = mock[EventConsumer]

    doReturn(10.0).when(p2).ipe
    doReturn(80.0).when(f2).ipe
    doReturn(10.0).when(c2).ipe

    val query2 = mock[Query]
  }


  "A DefaultOpScheduleStrategy" should "use the informed allocation strategy" in new Fixture {
    val schedStrategy = DefaultOpScheduleStrategy(allocStrategy)
    val it = schedStrategy.allocate(1000000.0, 0.0, 1, placement)

    it.next should be (a1)
    it.next should be (a2)
    it.next should be (a3)
    it.hasNext should be (false)
  }

  it should "consider a pending action between two actions" in new Fixture {

    // right before c1 execution
    val pendingActions = TreeSet[Action](EnqueueAction(c1, 800.0, 100.0))

    val schedStrategy = DefaultOpScheduleStrategy(allocStrategy)
    val it = schedStrategy.allocate(1000000.0, 0.0, 1, placement, pendingActions)

    it.next should be (a1)
    it.next should be (a2)
    it.next should be (EnqueueAction(c1, 800.0, 100.0))
    it.next should be (a3)
    it.hasNext should be (false)
  }

  it should "consider a pending action right in the beginning" in new Fixture {

    // right before p1 execution
    val pendingActions = TreeSet[Action](EnqueueAction(c1, 0.0, 100.0))

    val schedStrategy = DefaultOpScheduleStrategy(allocStrategy)
    val it = schedStrategy.allocate(1000000.0, 0.0, 1, placement, pendingActions)

    it.next should be (EnqueueAction(c1, 0.0, 100.0))
    it.next should be (a1)
    it.next should be (a2)
    it.next should be (a3)
    it.hasNext should be (false)
  }

  it should "consider a pending action in the middle of a execute action" in new Fixture {

    val pendingActions = TreeSet[Action](EnqueueAction(c1, 100.0, 100.0))

    val schedStrategy = DefaultOpScheduleStrategy(allocStrategy)
    val it = schedStrategy.allocate(1000000.0, 0.0, 1, placement, pendingActions)

    it.next should be (ExecuteAction(p1,   0.0, 100.0, 100000.0))
    it.next should be (EnqueueAction(c1, 100.0, 100.0))
    it.next should be (ExecuteAction(p1, 100.0, 200.0, 100000.0))
    it.next should be (a2)
    it.next should be (a3)
    it.hasNext should be (false)
  }


  it should "consider many pending actions" in new Fixture {
    val pendingActions = TreeSet[Action](EnqueueAction(p1,   0.0, 100.0),
                                         EnqueueAction(f1, 100.0,  50.0),
                                         EnqueueAction(c1, 800.0,  30.0))

    val schedStrategy = DefaultOpScheduleStrategy(allocStrategy)
    val it = schedStrategy.allocate(1000000.0, 0.0, 1, placement, pendingActions)

    it.next should be (EnqueueAction(p1,   0.0, 100.0))
    it.next should be (ExecuteAction(p1,   0.0, 100.0, 100000.0))
    it.next should be (EnqueueAction(f1, 100.0,  50.0))
    it.next should be (ExecuteAction(p1, 100.0, 200.0, 100000.0))
    it.next should be (a2)
    it.next should be (EnqueueAction(c1, 800.0,  30.0))
    it.next should be (a3)
    it.hasNext should be (false)
  }

  it should "not consider actions after the iteration end" in new Fixture {
    // right before p1 execution
    val pendingActions = TreeSet[Action](EnqueueAction(c1, 1000.0, 100.0), EnqueueAction(c1, 2000.0, 100.0))

    val schedStrategy = DefaultOpScheduleStrategy(allocStrategy)
    val it = schedStrategy.allocate(1000000.0, 0.0, 1, placement, pendingActions)

    it.next should be (a1)
    it.next should be (a2)
    it.next should be (a3)
    it.hasNext should be (false)
  }


}

