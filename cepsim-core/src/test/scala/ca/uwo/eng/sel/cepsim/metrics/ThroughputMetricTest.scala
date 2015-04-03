package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

/**
 * Created by virso on 15-03-02.
 */
@RunWith(classOf[JUnitRunner])
class ThroughputMetricTest  extends FlatSpec
  with Matchers
  with MockitoSugar {



  trait CommonFixture {
    val prod1 = mock[EventProducer]("prod1")
    val op1 = mock[Operator]("op1")
    val op2 = mock[Operator]("op2")
    val cons1 = mock[EventConsumer]("cons1")
    val q = mock[Query]

    doReturn(Set(q)).when(cons1).queries

    doReturn(Set(prod1)).when(op1).predecessors
    doReturn(Map(op1   -> 1.0)).when(prod1).selectivities

    val placement = mock[Placement]
    doReturn(1).when(placement).vmId
    doReturn(Set(prod1)).when(placement).producers
    doReturn(Set(cons1)).when(placement).consumers
  }

  /**
   * Fixture 1:
   *  prod1 -> op1 -> op2 -> cons1
   */
  trait Fixture1 extends CommonFixture {

    val path1 = mock[VertexPath]("path1")

    doReturn(prod1).when(path1).producer
    doReturn(List(path1)).when(q).pathsToProducers(cons1)

    doReturn(Set(op1)).when(op2).predecessors
    doReturn(Set(op2)).when(cons1).predecessors

    doReturn(Set(op1)).when(prod1).successors
    doReturn(Set(op2)).when(op1).successors
    doReturn(Set(cons1)).when(op2).successors

    doReturn(Map(op2   -> 0.1)).when(op1  ).selectivities
    doReturn(Map(cons1 -> 1.0)).when(op2  ).selectivities

    doReturn(Iterator(prod1, op1, op2, cons1)).when(placement).iterator
  }

  /**
   * Fixture 2:
   *
   *         /--> op1--\
   *  prod1 -           -> op3 -> cons1
   *         \--> op2--/
   */
  trait Fixture2 extends CommonFixture {
    val path1 = mock[VertexPath]("path1")
    val path2 = mock[VertexPath]("path2")

    doReturn(prod1).when(path1).producer
    doReturn(prod1).when(path2).producer
    doReturn(List(path1, path2)).when(q).pathsToProducers(cons1)

    val op3 = mock[Operator]("op3")

    doReturn(Set(prod1)).when(op2).predecessors
    doReturn(Set(op1, op2)).when(op3).predecessors
    doReturn(Set(op3)).when(cons1).predecessors

    doReturn(Set(op1, op2)).when(prod1).successors
    doReturn(Set(op3)).when(op1).successors
    doReturn(Set(op3)).when(op2).successors
    doReturn(Set(cons1)).when(op3).successors

    doReturn(Map(op1 -> 1.0, op2 -> 1.0)).when(prod1).selectivities
    doReturn(Map(op3 -> 0.1)).when(op1).selectivities
    doReturn(Map(op3 -> 0.2)).when(op2).selectivities
    doReturn(Map(cons1 -> 1.0)).when(op3).selectivities

    doReturn(Iterator(prod1, op1, op2, op3, cons1)).when(placement).iterator
  }

  /**
   * Fixture 3:
   *
   *  prod1 -> op1--\
   *                 -> op3 -> cons1
   *  prod2 -> op2--/
   */
  trait Fixture3 extends CommonFixture {
    val prod2 = mock[EventProducer]("prod2")
    val op3   = mock[Operator]("op3")
    val path1 = mock[VertexPath]("path1")
    val path2 = mock[VertexPath]("path2")

    doReturn(prod1).when(path1).producer
    doReturn(prod2).when(path2).producer
    doReturn(List(path1, path2)).when(q).pathsToProducers(cons1)

    doReturn(Set(prod2)).when(op2).predecessors
    doReturn(Set(op1, op2)).when(op3).predecessors
    doReturn(Set(op3)).when(cons1).predecessors

    doReturn(Set(op1)).when(prod1).successors
    doReturn(Set(op2)).when(prod2).successors
    doReturn(Set(op3)).when(op1).successors
    doReturn(Set(op3)).when(op2).successors
    doReturn(Set(cons1)).when(op3).successors

    doReturn(Map(op1 -> 1.0)).when(prod1).selectivities
    doReturn(Map(op2 -> 1.0)).when(prod2).selectivities
    doReturn(Map(op3 -> 0.1)).when(op1).selectivities
    doReturn(Map(op3 -> 0.2)).when(op2).selectivities
    doReturn(Map(cons1 -> 1.0)).when(op3).selectivities

    doReturn(Set(prod1, prod2)).when(placement).producers
    doReturn(Iterator(prod1, prod2, op1, op2, op3, cons1)).when(placement).iterator
  }

  /**
   * Fixture 4:
   *  prod1 -> w1 -> op2 -> cons1
   */
  trait Fixture4 extends CommonFixture {

    val path1 = mock[VertexPath]("path1")

    doReturn(prod1).when(path1).producer
    doReturn(List(path1)).when(q).pathsToProducers(cons1)

    val w1 = mock[WindowedOperator]("w1")
    doReturn(Set(prod1)).when(w1).predecessors
    doReturn(Set(w1)).when(op2).predecessors
    doReturn(Set(op2)).when(cons1).predecessors

    doReturn(Set(w1)).when(prod1).successors
    doReturn(Set(op2)).when(w1).successors
    doReturn(Set(cons1)).when(op2).successors

    doReturn(Map(w1    -> 1.0)).when(prod1).selectivities
    doReturn(Map(op2   -> 1.0)).when(w1   ).selectivities
    doReturn(Map(cons1 -> 1.0)).when(op2  ).selectivities

    doReturn(Iterator(prod1, w1, op2, cons1)).when(placement).iterator
  }



  "A ThroughputMetricCalculator" should "calculate the right throughput for one iteration" in new Fixture1 {
    val throughput = LatencyThroughputCalculator(placement)

    throughput update Generated (prod1, 10.0, 10.0)
    throughput update Produced(prod1, 11.0, 10.0, Map.empty)
    throughput update Produced(op1,   21.0, 10.0, Map(prod1 -> 10.0))
    throughput update Produced(op2,   22.0,  1.0, Map(op1   ->  1.0))
    throughput update Consumed (cons1, 23.0,  1.0, Map(op2   ->  1.0))

    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (1)

    results(0) should be (ThroughputMetric(cons1, 23.0, 10.0))

  }

  it should "calculate the right throughput for two iterations" in new Fixture1 {
    val throughput = LatencyThroughputCalculator(placement)

    throughput update Generated (prod1, 10.0, 30.0)
    throughput update Produced(prod1, 11.0, 30.0, Map.empty)
    throughput update Produced(op1,   21.0, 30.0, Map(prod1 -> 30.0))
    throughput update Produced(op2,   24.0,  3.0, Map(op1   ->  3.0))
    throughput update Consumed (cons1, 27.0,  3.0, Map(op2   ->  3.0))

    throughput update Generated (prod1, 30.0, 10.0)
    throughput update Produced(prod1, 31.0, 10.0, Map.empty)
    throughput update Produced(op1,   41.0, 10.0, Map(prod1 -> 10.0))
    throughput update Produced(op2,   44.0,  1.0, Map(op1   ->  1.0))
    throughput update Consumed (cons1, 47.0,  1.0, Map(op2   ->  1.0))

    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (2)

    results(0) should be (ThroughputMetric(cons1, 27.0, 30.0))
    results(1) should be (ThroughputMetric(cons1, 47.0, 40.0))
  }


  it should "calculate the right throughput when there are two paths for the same producer" in new Fixture2 {
    val throughput = LatencyThroughputCalculator(placement)

    throughput update Generated (prod1, 10.0, 10.0)
    throughput update Produced(prod1, 11.0, 10.0, Map.empty)
    throughput update Produced(op1,   21.0, 10.0, Map(prod1 -> 10.0))
    throughput update Produced(op2,   31.0, 10.0, Map(prod1 -> 10.0))
    throughput update Produced(op3,   31.0,  3.0, Map(op1 -> 1.0, op2 -> 2.0))
    throughput update Consumed(cons1, 42.0,  3.0, Map(op3 -> 3.0))


    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (1)

    results(0) should be (ThroughputMetric(cons1, 42.0, 10.0))
  }

  it should "calculate the right throughput when there are more than one producer" in new Fixture3 {
    val throughput = LatencyThroughputCalculator(placement)

    throughput update Generated (prod1, 10.0, 10.0)
    throughput update Produced(prod1, 20.0, 10.0, Map.empty)
    throughput update Generated (prod2, 20.0, 20.0)
    throughput update Produced(prod2, 30.0, 20.0, Map.empty)

    throughput update Produced(op1,   40.0, 10.0, Map(prod1 -> 10.0))
    throughput update Produced(op2,   50.0, 10.0, Map(prod2 -> 10.0))
    throughput update Produced(op3,   55.0,  5.0, Map(op1 -> 1.0, op2 -> 4.0))
    throughput update Consumed(cons1,  60.0,  5.0, Map(op3 -> 5.0))


    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (1)
    results(0) should be (ThroughputMetric(cons1, 60.0, 30.0))
  }


  it should "correctly handle windowed operators" in new Fixture4 {
    doReturn(1).when(w1).slots // 30 millisecond window

    val throughput = LatencyThroughputCalculator(placement)

    throughput.update(Generated        (prod1, 0.0, 10.0))
    throughput.update(Produced         (prod1, 1.0, 10.0))
    throughput.update(WindowAccumulated(w1, 2.0, 0, Map(prod1 -> 10.0)))

    throughput.update(Generated        (prod1, 10.0, 10.0))
    throughput.update(Produced         (prod1, 11.0, 10.0))
    throughput.update(WindowAccumulated(w1, 12.0, 0, Map(prod1 -> 10.0)))

    throughput.update(Generated        (prod1, 20.0, 10.0))
    throughput.update(Produced         (prod1, 21.0, 10.0))
    throughput.update(WindowProduced   (w1,    22.0, 1.0, 0))
    throughput.update(WindowAccumulated(w1,    22.0, 0, Map(prod1 -> 10.0)))
    throughput.update(Produced         (op2,   24.0, 1.0, Map(w1  -> 1)))
    throughput.update(Consumed         (cons1, 25.0, 1.0, Map(op2  -> 1)))

    throughput.update(Generated        (prod1, 40.0, 10.0))
    throughput.update(Produced         (prod1, 41.0, 10.0))
    throughput.update(WindowProduced   (w1,    42.0, 1.0, 0))
    throughput.update(WindowAccumulated(w1,    42.0, 0, Map(prod1 -> 10.0)))
    throughput.update(Produced         (op2,   44.0,  1.0, Map(w1   -> 1)))
    throughput.update(Consumed         (cons1, 45.0,  1.0, Map(op2  -> 1)))

    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (2)

    results(0) should be (ThroughputMetric(cons1, 25.0, 20.0))
    results(1) should be (ThroughputMetric(cons1, 45.0, 30.0))
  }


  it should "correctly handle window like operators that have the advance duration different from size" in new Fixture4 {
    doReturn(3).when(w1).slots // 30 millisecond window / 10 millisecond advance

    val throughput = LatencyThroughputCalculator(placement)

    // accumulated at slot 0
    throughput.update(Generated        (prod1,  0.0, 10.0))
    throughput.update(Produced         (prod1,  1.0, 10.0))
    throughput.update(WindowAccumulated(w1,     2.0, 0, Map(prod1 -> 10.0)))

    // accumulated at slot 1
    // slot 0 window has passed - generates an output
    throughput.update(Generated        (prod1, 10.0, 10.0))
    throughput.update(Produced         (prod1, 11.0, 10.0))
    throughput.update(WindowProduced   (w1,    12.0,  1.0, 0))
    throughput.update(WindowAccumulated(w1,    12.0,  1, Map(prod1 -> 10.0)))
    throughput.update(Produced         (op2,   14.0,  1.0, Map(w1  -> 1)))
    throughput.update(Consumed         (cons1, 15.0,  1.0, Map(op2 -> 1)))

    throughput.update(Generated        (prod1, 20.0, 10.0))
    throughput.update(Produced         (prod1, 21.0, 10.0))
    throughput.update(WindowProduced   (w1,    22.0,  2.0, 1))
    throughput.update(WindowAccumulated(w1,    22.0,  2, Map(prod1 -> 10.0)))
    throughput.update(Produced         (op2,   24.0,  2.0, Map(w1  -> 2)))
    throughput.update(Consumed         (cons1, 25.0,  2.0, Map(op2 -> 2)))

    throughput.update(Generated        (prod1, 30.0, 10.0))
    throughput.update(Produced         (prod1, 31.0, 10.0))
    throughput.update(WindowProduced   (w1,    32.0,  3.0, 2))
    throughput.update(WindowAccumulated(w1,    32.0,  0, Map(prod1 -> 10.0)))
    throughput.update(Produced         (op2,   34.0,  3.0, Map(w1  -> 3)))
    throughput.update(Consumed         (cons1, 35.0,  3.0, Map(op2 -> 3)))

    // next window only considers new events
    throughput.update(Generated        (prod1, 40.0, 10.0))
    throughput.update(Produced         (prod1, 41.0, 10.0))
    throughput.update(WindowProduced   (w1,    42.0,  3.0, 0))
    throughput.update(WindowAccumulated(w1,    42.0,  1, Map(prod1 -> 10.0)))
    throughput.update(Produced         (op2,   44.0,  3.0, Map(w1   -> 3)))
    throughput.update(Consumed         (cons1, 45.0,  3.0, Map(op2  -> 3)))


    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (4)

    results(0) should be (ThroughputMetric(cons1, 15.0, 10.0))
    results(1) should be (ThroughputMetric(cons1, 25.0, 20.0))
    results(2) should be (ThroughputMetric(cons1, 35.0, 30.0))
    results(3) should be (ThroughputMetric(cons1, 45.0, 40.0))
  }
}
