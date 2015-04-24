package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{Query, EventConsumer, Operator, EventProducer}
import ca.uwo.eng.sel.cepsim.sched.alloc.AllocationStrategy
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class DefaultOpScheduleStrategyTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val p1 = mock[EventProducer]
    val p2 = mock[EventProducer]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val c1 = mock[EventConsumer]
    val c2 = mock[EventConsumer]
    doReturn(10.0).when(p1).ipe
    doReturn(10.0).when(p2).ipe
    doReturn(80.0).when(f1).ipe
    doReturn(80.0).when(f2).ipe
    doReturn(10.0).when(c1).ipe
    doReturn(10.0).when(c2).ipe

    val query1 = mock[Query]
    val query2 = mock[Query]


  }

  "A DefaultOpScheduleStrategy" should "use the informed allocation strategy" in new Fixture {
    val placement = mock[Placement]
    doReturn(Iterator(p1, f1, c1)).when(placement).iterator

    val allocStrategy = mock[AllocationStrategy]
    doReturn(Map(p1 -> 200000.0, f1 -> 600000.0, c1 -> 200000.0)).when(allocStrategy).instructionsPerOperator(1000000.0, placement)

    val schedStrategy = DefaultOpScheduleStrategy(allocStrategy)
    val it = schedStrategy.allocate(1000000.0, 0.0, 1, placement)

    it.next should be (ExecuteAction(p1,   0.0,  200.0, 200000.0))
    it.next should be (ExecuteAction(f1, 200.0,  800.0, 600000.0))
    it.next should be (ExecuteAction(c1, 800.0, 1000.0, 200000.0))
    it.hasNext should be (false)

  }



}

