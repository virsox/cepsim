package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.history._
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class LatencyMetricTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait CommonFixture {
    val prod1 = mock[EventProducer]("prod1")
    val op1 = mock[Operator]("op1")
    val op2 = mock[Operator]("op2")
    val cons1 = mock[EventConsumer]("cons1")
    val q = mock[Query]
    doReturn(Set(q)).when(prod1).queries
    doReturn(Set(q)).when(op1).queries
    doReturn(Set(q)).when(op2).queries
    doReturn(Set(q)).when(cons1).queries

    doReturn(Map(op1 -> 1.0)).when(prod1).selectivities

    val placement = mock[Placement]
    doReturn(1).when(placement).vmId

    val path1 = mock[VertexPath]("path1")
    doReturn(prod1).when(path1).producer

  }

   /**
     * Fixture 1:
     *  prod1 -> op1 -> op2 -> cons1
     */
  trait Fixture1 extends CommonFixture {
    doReturn(List(path1)).when(q).pathsToProducers(cons1)

    doReturn(Map(op2 -> 1.0)).when(op1).selectivities
    doReturn(Map(cons1 -> 1.0)).when(op2).selectivities

    doReturn(Set.empty).when(prod1).predecessors
    doReturn(Set(prod1)).when(op1).predecessors
    doReturn(Set(op1)).when(op2).predecessors
    doReturn(Set(op2)).when(cons1).predecessors

    doReturn(Set(op1)).when(prod1).successors
    doReturn(Set(op2)).when(op1).successors
    doReturn(Set(cons1)).when(op2).successors

    doReturn(Iterator(prod1, op1, op2, cons1)).when(placement).iterator
    doReturn(Set(prod1)).when(placement).producers
    doReturn(Set(cons1)).when(placement).consumers
    doReturn(Set(prod1, op1, op2, cons1)).when(placement).vertices
  }

  /**
    * Fixture 2:
    *   prod1 -> op1 -\
    *                  - op3 -> cons1
    *   prod2 -> op2 -/
    */
  trait Fixture2 extends CommonFixture {

    val prod2 = mock[EventProducer]("prod2")
    val op3 = mock[Operator]("op3")

    doReturn(Map(op2 -> 1.0)).when(prod2).selectivities
    doReturn(Map(op3 -> 1.0)).when(op1).selectivities
    doReturn(Map(op3 -> 1.0)).when(op2).selectivities
    doReturn(Map(cons1 -> 1.0)).when(op3).selectivities

    doReturn(Set(prod1)).when(op1).predecessors
    doReturn(Set(prod2)).when(op2).predecessors
    doReturn(Set(op1, op2)).when(op3).predecessors
    doReturn(Set(op3)).when(cons1).predecessors

    doReturn(Set(op1)).when(prod1).successors
    doReturn(Set(op2)).when(prod2).successors
    doReturn(Set(op3)).when(op1).successors
    doReturn(Set(op3)).when(op2).successors
    doReturn(Set(cons1)).when(op3).successors

    doReturn(Iterator(prod1, op1, prod2, op2, op3, cons1)).when(placement).iterator
    doReturn(Set(prod1, prod2)).when(placement).producers
    doReturn(Set(cons1)).when(placement).consumers
    doReturn(Set(prod1, prod2, op1, op2, op3, cons1)).when(placement).vertices

    val path2 = mock[VertexPath]("path2")
    doReturn(prod2).when(path2).producer
    doReturn(List(path1, path2)).when(q).pathsToProducers(cons1)
  }



  "A LatencyMetricTest" should "calculate the correct latency for one iteration" in new Fixture1 {
    val latency = LatencyThroughputCalculator(placement)

    latency.update(Generated(prod1,  0.0, 10.0, 10.0))
    latency.update(Produced (prod1, 10.0, 11.0, 10.0))
    latency.update(Produced (op1,   11.0, 15.0, 10.0, Map(prod1 -> 10)))
    latency.update(Produced (op2,   15.0, 20.0, 10.0, Map(op1  -> 10)))
    latency.update(Consumed (cons1, 20.0, 25.0, 10.0, Map(op2  -> 10)))

    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (1)
    results.head should be (LatencyMetric(cons1, 25.0, 10.0, 15.0))
  }


  it should "calculate the correct latency for two iterations" in new Fixture1 {
    val latency = LatencyThroughputCalculator(placement)

    latency.update(Generated(prod1,  0.0, 10.0, 10.0))
    latency.update(Produced (prod1, 10.0, 11.0, 5.0))
    latency.update(Produced (op1,   11.0, 15.0, 5.0, Map(prod1 -> 5)))
    latency.update(Produced (op2,   15.0, 20.0, 5.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 20.0, 25.0, 5.0, Map(op2  -> 5)))

    // second iteration
    latency.update(Produced (prod1, 25.0, 31.0, 5.0))
    latency.update(Produced (op1,   31.0, 35.0, 5.0, Map(prod1 -> 5)))
    latency.update(Produced (op2,   35.0, 40.0, 5.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 40.0, 45.0, 5.0, Map(op2  -> 5)))

    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (2)

    results(0) should be (LatencyMetric(cons1, 25.0, 5.0, 15.0))
    results(1) should be (LatencyMetric(cons1, 45.0, 5.0, 35.0))
  }

  it should "calculate the correct latency for two iterations even if more events are produced" in new Fixture1 {
    val latency = LatencyThroughputCalculator(placement)

    latency.update(Generated(prod1,  0.0,  5.0, 10.0))
    latency.update(Produced (prod1,  5.0, 11.0,  5.0))
    latency.update(Produced (op1,   11.0, 15.0,  5.0, Map(prod1 -> 5)))
    latency.update(Produced (op2,   15.0, 20.0,  5.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 20.0, 25.0,  5.0, Map(op2  -> 5)))

    // second iteration
    latency.update(Generated(prod1,  5.0, 20.0, 10.0))
    latency.update(Produced (prod1, 25.0, 31.0,  5.0))
    latency.update(Produced (op1,   31.0, 35.0,  5.0, Map(prod1 -> 5)))
    latency.update(Produced (op2,   35.0, 40.0,  5.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 40.0, 45.0,  5.0, Map(op2  -> 5)))

    // third iteration
    latency.update(Produced(prod1, 45.0, 51.0, 5.0))
    latency.update(Produced(op1,   51.0, 55.0, 5.0, Map(prod1 -> 5)))
    latency.update(Produced(op2,   55.0, 60.0, 5.0, Map(op1  -> 5)))
    latency.update(Consumed(cons1, 60.0, 65.0, 5.0, Map(op2  -> 5)))

    // fourth iteration
    latency.update(Produced(prod1, 65.0, 71.0, 5.0))
    latency.update(Produced(op1,   71.0, 75.0, 5.0, Map(prod1 -> 5)))
    latency.update(Produced(op2,   75.0, 80.0, 5.0, Map(op1  -> 5)))
    latency.update(Consumed(cons1, 80.0, 85.0, 5.0, Map(op2  -> 5)))


    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (4)

    results(0) should be (LatencyMetric(cons1, 25.0, 5.0, 20.0))
    results(1) should be (LatencyMetric(cons1, 45.0, 5.0, 30.0))
    results(2) should be (LatencyMetric(cons1, 65.0, 5.0, 50.0))
    results(3) should be (LatencyMetric(cons1, 85.0, 5.0, 70.0))
  }


  it should "calculate the correct latency if one output needs more than one iteration" in new Fixture1 {
    val latency = LatencyThroughputCalculator(placement)

    latency.update(Generated(prod1,  0.0, 10.0, 10.0))
    latency.update(Produced (prod1, 10.0, 11.0, 10.0))
    latency.update(Produced (op1,   11.0, 15.0, 10.0, Map(prod1 -> 10.0)))
    latency.update(Produced (op2,   15.0, 20.0, 10.0, Map(op1   -> 10.0)))
    latency.update(Consumed (cons1, 20.0, 25.0,  0.0, Map(op2   ->  5.0)))
    latency.update(Consumed (cons1, 25.0, 30.0,  1.0, Map(op2   ->  5.0)))

    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (1)

    results(0) should be (LatencyMetric(cons1, 30.0, 1.0, 20.0))
  }

  it should "calculate the correct latency if an operator does not have input events but it is executed" in new Fixture1 {

    val latency = LatencyThroughputCalculator(placement)
    latency.update(Generated(prod1,  0.0, 10.0, 10.0))
    latency.update(Produced (prod1, 10.0, 11.0, 10.0))
    latency.update(Produced (op1,   11.0, 15.0, 10.0, Map(prod1 -> 10.0)))

    latency.update(Produced (op2,   15.0, 20.0,  0.0, Map(op1 -> 10.0)))
    latency.update(Produced (op2,   20.0, 25.0,  0.0, Map(op1 ->  0.0)))
    latency.update(Produced (op2,   25.0, 30.0,  1.0, Map(op1 ->  0.0)))
    latency.update(Consumed (cons1, 30.0, 40.0,  1.0, Map(op2 ->  1.0)))

    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (1)
    results(0) should be (LatencyMetric(cons1, 40.0, 1.0, 30.0))
  }


  it should "correctly handle selectivities larger than one" in new Fixture1 {
    doReturn(Map(op2 -> 5.0)).when(op1).selectivities

    val latency = LatencyThroughputCalculator(placement)

    latency.update(Generated(prod1,  0.0, 10.0, 10.0))
    latency.update(Produced (prod1, 10.0, 11.0, 10.0))
    latency.update(Produced (op1,   11.0, 15.0, 10.0, Map(prod1 -> 10)))
    latency.update(Produced (op2,   15.0, 20.0, 50.0, Map(op1  -> 50)))
    latency.update(Consumed (cons1, 20.0, 25.0, 50.0, Map(op2  -> 50)))

    latency.update(Generated(prod1, 25.0, 30.0, 10.0))
    latency.update(Produced (prod1, 30.0, 31.0, 10.0))
    latency.update(Produced (op1,   31.0, 35.0, 10.0, Map(prod1 -> 10)))
    latency.update(Produced (op2,   35.0, 40.0, 50.0, Map(op1  -> 50)))
    latency.update(Consumed (cons1, 40.0, 45.0, 50.0, Map(op2  -> 50)))

    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (2)

    results(0) should be (LatencyMetric(cons1, 25.0, 50.0, 15.0))
    results(1) should be (LatencyMetric(cons1, 45.0, 50.0, 15.0))
  }

  it should "handle consumers that have not consumed events" in new Fixture1 {
    val latency = LatencyThroughputCalculator(placement)

    latency.update(Generated(prod1,  0.0, 10.0, 1.0))
    latency.update(Produced (prod1, 10.0, 11.0, 1.0))
    latency.update(Produced (op1,   11.0, 15.0, 1.0, Map(prod1 -> 1)))
    latency.update(Produced (op2,   15.0, 20.0, 1.0, Map(op1   -> 1)))
    latency.update(Consumed (cons1, 20.0, 25.0, 0.0, Map(op2  ->  0.5)))
    latency.update(Consumed (cons1, 26.0, 30.0, 1.0, Map(op2  ->  0.5)))

    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (1)

    results(0) should be (LatencyMetric(cons1, 30.0, 1.0, 20.0))

  }

  it should "calculate the latency when operators receive events at different timing" in new Fixture2 {
    val latency = LatencyThroughputCalculator(placement)

    latency.update(Generated(prod1, 0.0, 10.0, 10.0))
    latency.update(Generated(prod2, 0.0, 12.0, 10.0))

    latency.update(Produced (prod1, 10.0, 11.0, 10.0))
    latency.update(Produced (op1,   11.0, 15.0, 10.0, Map(prod1 -> 10.0))) // latency 5

    latency.update(Produced (prod2, 15.0, 16.0, 10.0))
    latency.update(Produced (op2,   16.0, 20.0, 10.0, Map(prod2 -> 10.0))) // latency 8

    // latency (10 * (5 + 25 - 15) + 10 * (8 + 25 - 20) / 20 = 14
    latency.update(Produced (op3,   20.0, 25.0, 20.0, Map(op1 -> 10.0, op2 -> 10.0)))
    latency.update(Consumed (cons1, 25.0, 30.0, 20.0, Map(op3   -> 20.0)))

    val results = latency.results(LatencyMetric.ID, cons1)
    results should have size (1)

    results(0) should be (LatencyMetric(cons1, 30.0, 20.0, 19.0))
  }



}



