package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.history._
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 15-04-04.
 */
class LatencyMetricWindowOpTest extends FlatSpec
  with Matchers
  with MockitoSugar  {

  /**
   * Fixture 1:
   *  prod1 -> w1 -> op2 -> cons1
   */
  trait Fixture {
    val prod1 = mock[EventProducer]("prod1")
    val w1 = mock[WindowedOperator]("w1")
    val op2 = mock[Operator]("op2")
    val cons1 = mock[EventConsumer]("cons1")
    val q = mock[Query]

    doReturn(Set(q)).when(prod1).queries
    doReturn(Set(prod1)).when(w1).predecessors
    doReturn(Set(q)).when(op2).queries
    doReturn(Set(q)).when(cons1).queries

    doReturn(Map(w1 -> 1.0)).when(prod1).selectivities
    doReturn(Map(op2 -> 1.0)).when(w1).selectivities
    doReturn(Map(cons1 -> 1.0)).when(op2).selectivities

    doReturn(Set.empty).when(prod1).predecessors
    doReturn(Set(w1)).when(op2).predecessors
    doReturn(Set(op2)).when(cons1).predecessors

    doReturn(Set(w1)).when(prod1).successors
    doReturn(Set(op2)).when(w1).successors
    doReturn(Set(cons1)).when(op2).successors

    val placement = mock[Placement]
    doReturn(1).when(placement).vmId
    doReturn(Iterator(prod1, w1, op2, cons1)).when(placement).iterator
    doReturn(Set(prod1)).when(placement).producers
    doReturn(Set(cons1)).when(placement).consumers
    doReturn(Set(prod1, w1, op2, cons1)).when(placement).vertices

    val path1 = mock[VertexPath]("path1")
    doReturn(prod1).when(path1).producer
    doReturn(List(path1)).when(q).pathsToProducers(cons1)

  }

  "LatencyMetric calculator" should "correctly handle windowed operators" in new Fixture {
    doReturn(1).when(w1).slots // 20 millisecond window


    val latency = LatencyThroughputCalculator(placement)

    latency.update(Generated        (prod1,  0.0, 0.0, 10.0))
    latency.update(Produced         (prod1,  0.0, 1.0, 10.0))
    latency.update(WindowAccumulated(w1,     1.0, 2.0, 0, Map(prod1 -> 10.0)))

    latency.update(Generated        (prod1,  2.0, 10.0, 10.0))
    latency.update(Produced         (prod1, 10.0, 11.0, 10.0))
    latency.update(WindowAccumulated(w1,    11.0, 12.0, 0, Map(prod1 -> 10.0)))

    latency.update(Generated        (prod1, 12.0, 20.0, 10.0))
    latency.update(Produced         (prod1, 20.0, 21.0, 10.0))
    latency.update(WindowProduced   (w1,    21.0, 22.0,  1.0, 0))
    latency.update(WindowAccumulated(w1,    21.0, 22.0,    0, Map(prod1 -> 10.0)))
    latency.update(Produced         (op2,   22.0, 24.0,  1.0, Map(w1  -> 1)))
    latency.update(Consumed         (cons1, 24.0, 25.0,  1.0, Map(op2  -> 1)))

    latency.update(Generated        (prod1, 25.0, 40.0, 10.0))
    latency.update(Produced         (prod1, 40.0, 41.0, 10.0))
    latency.update(WindowProduced   (w1,    41.0, 42.0,  1.0, 0))
    latency.update(WindowAccumulated(w1,    41.0, 42.0,    0, Map(prod1 -> 10.0)))
    latency.update(Produced         (op2,   42.0, 44.0,  1.0, Map(w1   -> 1)))
    latency.update(Consumed         (cons1, 44.0, 45.0,  1.0, Map(op2  -> 1)))

    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (2)

    results(0) should be (LatencyMetric(cons1, 25.0, 1.0, 20.0))
    results(1) should be (LatencyMetric(cons1, 45.0, 1.0, 25.0))
  }


  it should "handle windowed operators that have the advance duration different from size" in new Fixture {
    doReturn(3).when(w1).slots // 30 millisecond window / 10 millisecond advance

    val latency = LatencyThroughputCalculator(placement)

    // accumulated at slot 0
    latency.update(Generated        (prod1,  0.0,  0.0, 10.0))
    latency.update(Produced         (prod1,  0.0,  1.0, 10.0))
    latency.update(WindowAccumulated(w1,     1.0,  2.0, 0, Map(prod1 -> 10.0)))

    // accumulated at slot 1
    // slot 0 window has passed - generates an output
    latency.update(Generated        (prod1,  2.0, 10.0, 10.0))
    latency.update(Produced         (prod1, 10.0, 11.0, 10.0))
    latency.update(WindowProduced   (w1,    11.0, 12.0,  1.0, 0))
    latency.update(WindowAccumulated(w1,    11.0, 12.0,    1, Map(prod1 -> 10.0)))
    latency.update(Produced         (op2,   12.0, 14.0,  1.0, Map(w1  -> 1)))
    latency.update(Consumed         (cons1, 14.0, 15.0,  1.0, Map(op2 -> 1)))

    latency.update(Generated        (prod1, 15.0, 20.0, 10.0))
    latency.update(Produced         (prod1, 20.0, 21.0, 10.0))
    latency.update(WindowProduced   (w1,    21.0, 22.0,  2.0, 1))
    latency.update(WindowAccumulated(w1,    21.0, 22.0,    2, Map(prod1 -> 10.0)))
    latency.update(Produced         (op2,   22.0, 24.0,  2.0, Map(w1  -> 2)))
    latency.update(Consumed         (cons1, 24.0, 25.0,  2.0, Map(op2 -> 2)))

    latency.update(Generated        (prod1, 25.0, 30.0, 10.0))
    latency.update(Produced         (prod1, 30.0, 31.0, 10.0))
    latency.update(WindowProduced   (w1,    31.0, 32.0,  3.0, 2))
    latency.update(WindowAccumulated(w1,    31.0, 32.0,    0, Map(prod1 -> 10.0)))
    latency.update(Produced         (op2,   32.0, 34.0,  3.0, Map(w1  -> 3)))
    latency.update(Consumed         (cons1, 34.0, 35.0,  3.0, Map(op2 -> 3)))

    // next window only considers new events
    latency.update(Generated        (prod1, 35.0, 40.0, 10.0))
    latency.update(Produced         (prod1, 40.0, 41.0, 10.0))
    latency.update(WindowProduced   (w1,    41.0, 42.0,  3.0, 0))
    latency.update(WindowAccumulated(w1,    41.0, 42.0,    1, Map(prod1 -> 10.0)))
    latency.update(Produced         (op2,   42.0, 44.0,  3.0, Map(w1   -> 3)))
    latency.update(Consumed         (cons1, 44.0, 45.0,  3.0, Map(op2  -> 3)))


    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (4)

    results(0) should be (LatencyMetric(cons1, 15.0, 1.0, 15.0))
    results(1) should be (LatencyMetric(cons1, 25.0, 2.0, 20.0))
    results(2) should be (LatencyMetric(cons1, 35.0, 3.0, 25.0))
    results(3) should be (LatencyMetric(cons1, 45.0, 3.0, 25.0))
  }


  it should "handle windowed operators when more than one window has passed since last execution " in new Fixture {
    doReturn(5).when(w1).slots // 50 millisecond window / 10 millisecond advance

    val latency = LatencyThroughputCalculator(placement)
    (0 until 5).foreach((i) => {
      val start = i * 10
      val end   = (i + 1) * 10
      val quantity = i * 10

      latency.update(Generated        (prod1, start,       start,       10.0))
      latency.update(Produced         (prod1, start,       start + 1.0, 10.0))
      latency.update(WindowProduced   (w1,    start + 1.0, start + 2.0, quantity, i - 1))
      latency.update(WindowAccumulated(w1,    start + 1.0, start + 2.0, i, Map(prod1 -> 10)))
      if (quantity > 0) {
        latency.update(Produced       (op2,   start + 2.0, start + 4.0, quantity, Map(w1    -> quantity)))
        latency.update(Consumed       (cons1, start + 4.0, start + 5.0, quantity, Map(op2   -> quantity)))
      }
    })

    // 50 refers to the window (0 - 50), 40 to (10 - 60), 30 to (20 - 70)
    latency.update(WindowProduced   (w1,    45.0, 72.0, 50, 4))
    latency.update(WindowProduced   (w1,    45.0, 72.0, 40, 0))
    latency.update(WindowProduced   (w1,    45.0, 72.0, 30, 1))
    latency.update(WindowAccumulated(w1,    45.0, 72.0, 2, Map(prod1 -> 10.0)))
    latency.update(Produced         (op2,   72.0, 74.0, 120.0, Map(w1  -> 120)))
    latency.update(Consumed         (cons1, 74.0, 75.0, 120.0, Map(op2 -> 120)))

    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (5)

    // 1st window ( 0 - 50) - latency = 75 - 20 = 55
    // 2nd window (10 - 60) - latency = 75 - 25 = 50
    // 3rd window (20 - 70) - latency = 75 - 30 = 45
    // avg latency = (50 * 55 + 40 * 50 + 30 * 45) / 120 = 50.83333....
    results(4).time  should be (75.00)
    results(4).value should be (50.833 +- 0.001)
    results(4).asInstanceOf[LatencyMetric].quantity should be (120.0)
  }

}
