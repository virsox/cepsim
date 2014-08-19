package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{Query, EventConsumer, Operator, EventProducer}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

/**
 * Created by virso on 2014-08-13.
 */
@RunWith(classOf[JUnitRunner])
class UniformOpScheduleStrategyTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  "A RRScheduleStrategy" should "allocate the same number of instructions for every operator" in {
    val p1 = mock[EventProducer]
    val p2 = mock[EventProducer]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val c1 = mock[EventConsumer]
    val c2 = mock[EventConsumer]

    val placement = mock[Placement]
    doReturn(Set(p1, p2, f1, f2, c1, c2)).when(placement).vertices
    doReturn(Iterator(p1, p2, f1, f2, c1, c2)).when(placement).iterator

    val strategy = UniformOpScheduleStrategy()
    val ret = strategy.allocate(1200, placement)

    ret.next    should be ((p1, 200))
    ret.next    should be ((p2, 200))
    ret.next    should be ((f1, 200))
    ret.next    should be ((f2, 200))
    ret.next    should be ((c1, 200))
    ret.next    should be ((c2, 200))
    ret.hasNext should be (false)
  }

}
