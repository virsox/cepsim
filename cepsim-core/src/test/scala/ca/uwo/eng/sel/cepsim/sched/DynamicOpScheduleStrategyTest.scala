package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

/**
 * Created by virso on 2014-08-15.
 */
@RunWith(classOf[JUnitRunner])
class DynamicOpScheduleStrategyTest extends FlatSpec
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

    val strategy = new DynamicOpScheduleStrategy()
    val ret = strategy.allocate(1000, placement)

    doReturn(1.0).when(p1).ipe
    doReturn(1.0).when(f1).ipe
    doReturn(1.0).when(f2).ipe
    doReturn(1.0).when(c1).ipe

    doReturn(250.0).when(p1).inputQueue
    ret.next should be ((p1, 250))

    doReturn(250.0).when(f1).totalInputEvents
    ret.next should be ((f1, 250))

    doReturn(250.0).when(f2).totalInputEvents
    ret.next should be ((f2, 250))

    doReturn(250.0).when(c1).totalInputEvents
    ret.next should be ((c1, 250))
  }

  it should "reschedule operators if there are remaining instructions" in new Fixture {

    val strategy = new DynamicOpScheduleStrategy()
    val ret = strategy.allocate(1000, placement)

    doReturn(1.0).when(p1).ipe
    doReturn(1.0).when(f1).ipe
    doReturn(5.0).when(f2).ipe
    doReturn(1.0).when(c1).ipe

    doReturn(150.0).when(p1).inputQueue
    doReturn(150.0).when(f1).totalInputEvents
    doReturn(150.0).when(f2).totalInputEvents
    doReturn( 50.0).when(c1).totalInputEvents

    ret.next should be ((p1, 150))
    ret.next should be ((f1, 150))
    ret.next should be ((f2, 250))
    ret.next should be ((c1, 50))

    // ----- round 2
    doReturn(  0.0).when(p1).inputQueue
    doReturn(  0.0).when(f1).totalInputEvents
    doReturn(100.0).when(f2).totalInputEvents
    ret.next should be ((f2, 250))

    doReturn(50.0).when(f2).totalInputEvents
    doReturn(50.0).when(c1).totalInputEvents
    ret.next should be ((c1, 50))

    // ---- round 3
    ret.next should be ((f2, 100))

  }


}
