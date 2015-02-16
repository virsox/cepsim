package ca.uwo.eng.sel.cepsim.metrics

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{Query, EventConsumer, Operator, EventProducer}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

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
  }

  trait Fixture1 extends CommonFixture {
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
    doReturn(Set(prod1, op1, op2, cons1)).when(placement).vertices
  }

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
    doReturn(Set(prod1, prod2, op1, op2, op3, cons1)).when(placement).vertices
  }



  "A LatencyMetricTest" should "calculate the correct latency for an iteration" in new Fixture1 {
    val latency = LatencyMetric.calculator(placement)

    latency.update(Produced (prod1, 10.0, 0.0, 10.0))
    latency.update(Processed(prod1, 10.0, 11))
    latency.update(Processed(op1,  10.0, 15.0, Map(prod1 -> 10)))
    latency.update(Processed(op2,  10.0, 20.0, Map(op1  -> 10)))
    latency.update(Consumed (cons1, 10.0, 25.0, Map(op2  -> 10)))

    val results = latency.results
    results should have size (1)

    results.head should be (LatencyMetric(cons1, 25.0, 10.0, 20.0))
  }


  it should "calculate the correct latency for two iterations" in new Fixture1 {
    val latency = LatencyMetric.calculator(placement)

    latency.update(Produced (prod1, 10.0, 0.0, 10.0))
    latency.update(Processed(prod1, 5.0, 11))
    latency.update(Processed(op1,  5.0, 15.0, Map(prod1 -> 5)))
    latency.update(Processed(op2,  5.0, 20.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 5.0, 25.0, Map(op2  -> 5)))

    // second iteration
    latency.update(Processed(prod1, 5.0, 31))
    latency.update(Processed(op1,  5.0, 35.0, Map(prod1 -> 5)))
    latency.update(Processed(op2,  5.0, 40.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 5.0, 45.0, Map(op2  -> 5)))

    val results = latency.results
    results should have size (2)

    results(0) should be (LatencyMetric(cons1, 25.0, 5.0, 20.0))
    results(1) should be (LatencyMetric(cons1, 45.0, 5.0, 40.0))
  }

  it should "calculate the correct latency for two iterations even if more events are produced" in new Fixture1 {
    val latency = LatencyMetric.calculator(placement)

    latency.update(Produced (prod1, 10.0, 0.0, 10.0))
    latency.update(Processed(prod1, 5.0, 11))
    latency.update(Processed(op1,  5.0, 15.0, Map(prod1 -> 5)))
    latency.update(Processed(op2,  5.0, 20.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 5.0, 25.0, Map(op2  -> 5)))

    // second iteration
    latency.update(Produced (prod1, 10.0, 10.0, 30.0))
    latency.update(Processed(prod1, 5.0, 31))
    latency.update(Processed(op1,  5.0, 35.0, Map(prod1 -> 5)))
    latency.update(Processed(op2,  5.0, 40.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 5.0, 45.0, Map(op2  -> 5)))

    // third iteration
    latency.update(Processed(prod1, 5.0, 51))
    latency.update(Processed(op1,  5.0, 55.0, Map(prod1 -> 5)))
    latency.update(Processed(op2,  5.0, 60.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 5.0, 65.0, Map(op2  -> 5)))

    // fourth iteration
    latency.update(Processed(prod1, 5.0, 71))
    latency.update(Processed(op1,  5.0, 75.0, Map(prod1 -> 5)))
    latency.update(Processed(op2,  5.0, 80.0, Map(op1  -> 5)))
    latency.update(Consumed (cons1, 5.0, 85.0, Map(op2  -> 5)))


    val results = latency.results
    results should have size (4)

    results(0) should be (LatencyMetric(cons1, 25.0, 5.0, 20.0))
    results(1) should be (LatencyMetric(cons1, 45.0, 5.0, 30.0))
    results(2) should be (LatencyMetric(cons1, 65.0, 5.0, 50.0))
    results(3) should be (LatencyMetric(cons1, 85.0, 5.0, 70.0))
  }

  it should "calculate the correct latency if one output needs more than one iteration" in new Fixture1 {
    val latency = LatencyMetric.calculator(placement)

    latency.update(Produced (prod1, 10.0, 0.0, 10.0))
    latency.update(Processed(prod1, 10.0, 11))
    latency.update(Processed(op1,   10.0, 15.0, Map(prod1 -> 10.0)))
    latency.update(Processed(op2,   10.0, 20.0, Map(op1   -> 10.0)))
    latency.update(Consumed (cons1,  0.0, 25.0, Map(op2   ->  5.0)))
    latency.update(Consumed (cons1,  1.0, 30.0, Map(op2   ->  5.0)))

    val results = latency.results
    results should have size (1)

    results(0) should be (LatencyMetric(cons1, 30.0, 1.0, 25.0))
  }

  it should "correctly handle selectivities larger than one" in new Fixture1 {
    doReturn(Map(op2 -> 5.0)).when(op1).selectivities

    val latency = LatencyMetric.calculator(placement)

    latency.update(Produced (prod1, 10.0, 0.0, 10.0))
    latency.update(Processed(prod1, 10.0, 11))
    latency.update(Processed(op1,   10.0, 15.0, Map(prod1 -> 10)))
    latency.update(Processed(op2,   50.0, 20.0, Map(op1  -> 50)))
    latency.update(Consumed (cons1, 50.0, 25.0, Map(op2  -> 50)))

    latency.update(Produced (prod1, 10.0, 10.0, 30.0))
    latency.update(Processed(prod1, 10.0, 31.0))
    latency.update(Processed(op1,   10.0, 35.0, Map(prod1 -> 10)))
    latency.update(Processed(op2,   50.0, 40.0, Map(op1  -> 50)))
    latency.update(Consumed (cons1, 50.0, 45.0, Map(op2  -> 50)))

    val results = latency.results
    results should have size (2)

    results(0) should be (LatencyMetric(cons1, 25.0, 50.0, 20.0))
    results(1) should be (LatencyMetric(cons1, 45.0, 50.0, 25.0))
  }


  it should "correctly handle window like operators" in new Fixture1 {

    val latency = LatencyMetric.calculator(placement)

    latency.update(Produced (prod1, 10.0, 0.0, 10.0))
    latency.update(Processed(prod1, 10.0, 11))
    latency.update(Processed(op1,   0.0, 15.0, Map(prod1 -> 10.0)))

    latency.update(Produced (prod1, 10.0, 10.0, 20.0))
    latency.update(Processed(prod1, 10.0, 21.0))
    latency.update(Processed(op1,   0.0, 25.0, Map(prod1 -> 10.0)))

    latency.update(Produced (prod1, 10.0, 20.0, 30.0))
    latency.update(Processed(prod1, 10.0, 31.0))
    latency.update(Processed(op1,   1.0, 35.0, Map(prod1 -> 10.0)))
    latency.update(Processed(op2,   1.0, 40.0, Map(op1  -> 1)))
    latency.update(Consumed (cons1,  1.0, 45.0, Map(op2  -> 1)))

    // next window only considers new events
    latency.update(Produced (prod1, 10.0, 30.0, 40.0))
    latency.update(Processed(prod1, 10.0, 41.0))
    latency.update(Processed(op1,   1.0, 45.0, Map(prod1 -> 10.0)))
    latency.update(Processed(op2,   1.0, 50.0, Map(op1  -> 1)))
    latency.update(Consumed (cons1,  1.0, 55.0, Map(op2  -> 1)))


    val results = latency.results
    results should have size (2)

    results(0) should be (LatencyMetric(cons1, 45.0, 1.0, 30.0))
    results(1) should be (LatencyMetric(cons1, 55.0, 1.0, 20.0))

  }

  it should "calculate the latency when operators receive events at different timing" in new Fixture2 {
    val latency = LatencyMetric.calculator(placement)

    latency.update(Produced (prod1, 10.0, 0.0, 10.0))
    latency.update(Produced (prod2, 10.0, 6.0, 10.0))

    latency.update(Processed(prod1, 10.0, 11.0))
    latency.update(Processed(op1,   10.0, 15.0, Map(prod1 -> 10.0))) // latency 10

    latency.update(Processed(prod2, 10.0, 16.0))
    latency.update(Processed(op2,   10.0, 20.0, Map(prod2 -> 10.0))) // latency 12

    latency.update(Processed(op3,   20.0, 25.0, Map(op1 -> 10.0, op2 -> 10.0))) // latency (10 * 20 + 10 * 17) / 20 = 18.5
    latency.update(Consumed (cons1, 20.0, 30.0, Map(op3   -> 20.0)))

    val results = latency.results
    results should have size (1)

    results(0) should be (LatencyMetric(cons1, 30.0, 20.0, 23.5))
  }
  
  
}


