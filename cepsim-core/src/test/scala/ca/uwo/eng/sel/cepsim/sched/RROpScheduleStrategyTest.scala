package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 15-01-30.
 */
@RunWith(classOf[JUnitRunner])
class RROpScheduleStrategyTest extends FlatSpec
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

    doReturn(Set(query1)).when(placement).queries
    doReturn(Set(p1, f1, c1)).when(placement).vertices
    doReturn(Set(p1, f1, c1)).when(placement).vertices(query1)

    doAnswer(new Answer[Iterator[Vertex]] {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[Vertex] = Iterator(p1, f1, c1)
    }).when(placement).iterator
  }


  "A RROpScheduleStrategy" should "iterate N times over the vertices" in new Fixture {

    val strategy = RROpScheduleStrategy(10)
    val ret = strategy.allocate(1000, placement)

    for (i <- 1 to 10) {
      ret.hasNext should be (true)
      ret.next    should be ((p1, 10.0))
      ret.hasNext should be (true)
      ret.next    should be ((f1, 80.0))
      ret.hasNext should be (true)
      ret.next    should be ((c1, 10.0))
    }
    ret.hasNext should be (false)
  }


  it should "use all instructions on the last iterator" in new Fixture {
    val strategy = RROpScheduleStrategy(3)
    val ret = strategy.allocate(1000, placement)

    for (i <- 1 to 2) {
      ret.hasNext should be (true)
      ret.next    should be ((p1, 33.0))
      ret.hasNext should be (true)
      ret.next    should be ((f1, 266.0))
      ret.hasNext should be (true)
      ret.next    should be ((c1, 33.0))
    }
    ret.hasNext should be (true)
    ret.next    should be ((p1, 34.0))
    ret.hasNext should be (true)
    ret.next    should be ((f1, 268.0))
    ret.hasNext should be (true)
    ret.next    should be ((c1, 34.0))

    ret.hasNext should be (false)
  }


}
