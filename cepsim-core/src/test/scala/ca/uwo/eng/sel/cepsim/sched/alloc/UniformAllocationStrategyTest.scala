package ca.uwo.eng.sel.cepsim.sched.alloc

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, Operator, EventProducer}
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2015-04-23.
 */
@RunWith(classOf[JUnitRunner])
class UniformAllocationStrategyTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  "A UniformAllocationStrategy" should "allocate the same number of instructions for every operator" in {
    val p1 = mock[EventProducer]
    val p2 = mock[EventProducer]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val c1 = mock[EventConsumer]
    val c2 = mock[EventConsumer]

    val placement = mock[Placement]
    doReturn(Set(p1, p2, f1, f2, c1, c2)).when(placement).vertices
    doReturn(Iterator(p1, p2, f1, f2, c1, c2)).when(placement).iterator

    val strategy = UniformAllocationStrategy.apply()
    val ret = strategy.instructionsPerOperator(1200, placement)

    ret should have size (6)
    ret should be (Map(p1 -> 200.0, p2 -> 200.0, f1 -> 200.0,
                       f2 -> 200.0, c1 -> 200.0, c2 -> 200.0))
  }


}
