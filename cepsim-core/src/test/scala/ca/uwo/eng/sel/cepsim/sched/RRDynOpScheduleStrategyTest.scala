package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.alloc.WeightedAllocationStrategy
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

import scala.concurrent.duration._

/**
 * Created by virso on 15-02-01.
 */
@RunWith(classOf[JUnitRunner])
class RRDynOpScheduleStrategyTest extends FlatSpec
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

  "A RRDynOpScheduleStrategy" should "iterate N times over the vertices" in new Fixture {
    doReturn(100.0).when(p1).inputQueue
    doReturn(100.0).when(f1).totalInputEvents
    doReturn(100.0).when(c1).totalInputEvents

    val strategy = RRDynOpScheduleStrategy(WeightedAllocationStrategy(), 5)
    val ret = strategy.allocate(1000, placement)

    // if there are events to be processed, then the strategy works as the RRSchedulingStrategy
    for (i <- 1 to 5) {
      ret.hasNext should be (true)
      ret.next    should be ((p1, 20.0))
      ret.hasNext should be (true)
      ret.next    should be ((f1, 160.0))
      ret.hasNext should be (true)
      ret.next    should be ((c1, 20.0))
    }
    ret.hasNext should be (false)
  }

  it should "skip vertices without events to be processed" in new Fixture {
    doReturn(100.0).when(p1).inputQueue
    doReturn(100.0).when(f1).totalInputEvents
    doReturn(100.0).when(c1).totalInputEvents

    val strategy = RRDynOpScheduleStrategy(WeightedAllocationStrategy(), 4)
    val ret = strategy.allocate(1000, placement)


    // if there are events to be processed, then the strategy works as the RRSchedulingStrategy
    ret.next    should be ((p1, 25.0))
    ret.next    should be ((f1, 200.0))
    ret.next    should be ((c1, 25.0))

    doReturn(  0.0).when(p1).inputQueue
    doReturn(100.0).when(f1).totalInputEvents
    doReturn(100.0).when(c1).totalInputEvents
    ret.next    should be ((f1, 200.0))
    ret.next    should be ((c1, 25.0))

    doReturn(  0.0).when(p1).inputQueue
    doReturn(  0.0).when(f1).totalInputEvents
    doReturn(100.0).when(c1).totalInputEvents
    ret.next    should be ((c1, 25.0))

    doReturn(  0.0).when(p1).inputQueue
    doReturn(  0.0).when(f1).totalInputEvents
    doReturn(  0.0).when(c1).totalInputEvents

    ret.hasNext should be (false)

  }

  it should "calculate the number of iterations based on the aimed iteration length" in new Fixture {
    doReturn(100.0).when(p1).inputQueue
    doReturn(100.0).when(f1).totalInputEvents
    doReturn(100.0).when(c1).totalInputEvents

    val strategy = RRDynOpScheduleStrategy(WeightedAllocationStrategy(), 0.1 millisecond, 1000.0)

    // 10 millions instructions is a 10 ms simulation tick
    // the goal is to have an iteration of 0.1 ms (100,000 instructions).
    // Therefore, in total we need 100 iterations to complete 10ms
    var ret = strategy.allocate(10000000, placement)
    for (i <- 1 to 100) {
      ret.hasNext should be (true)
      ret.next    should be ((p1, 10000.0))
      ret.hasNext should be (true)
      ret.next    should be ((f1, 80000.0))
      ret.hasNext should be (true)
      ret.next    should be ((c1, 10000.0))
    }
    ret.hasNext should be (false)

    // now there are only 5 millions instructions on the tick - 5 ms simulation tick
    // If the iteration has 0.1ms (100,000 instructions), in total we need 50 iteratios
    // to complete 5ms
    ret = strategy.allocate(5000000, placement)
    for (i <- 1 to 50) {
      ret.hasNext should be (true)
      ret.next    should be ((p1, 10000.0))
      ret.hasNext should be (true)
      ret.next    should be ((f1, 80000.0))
      ret.hasNext should be (true)
      ret.next    should be ((c1, 10000.0))
    }
    ret.hasNext should be (false)

  }


}
