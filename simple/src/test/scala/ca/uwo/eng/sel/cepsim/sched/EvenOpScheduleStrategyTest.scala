package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, Operator, EventProducer}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class EvenOpScheduleStrategyTest  extends FlatSpec
  with Matchers
  with MockitoSugar {

  "A EvenOpScheduleStrategy" should "allocate same amount of MIPS for operators in the same Placement" in {

    val strategy = new EvenOpScheduleStrategy()
    val p1 = mock[EventProducer]
    doReturn(10.0).when(p1).ipe

    val f1 = mock[Operator]
    doReturn(80.0).when(f1).ipe

    val c1 = mock[EventConsumer]
    doReturn(10.0).when(c1).ipe

    val placement1 = mock[Placement]
    doReturn(Set(p1, f1, c1)).when(placement1).vertices

    val ret = strategy.allocate(1000, placement1)

    ret should have size (3)
    ret(p1) should be (100.0)
    ret(f1) should be (800.0)
    ret(c1) should be (100.0)

  }
}
