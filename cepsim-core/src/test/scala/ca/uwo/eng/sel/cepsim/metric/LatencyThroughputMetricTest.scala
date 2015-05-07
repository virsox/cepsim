package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.event.EventSet
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
class LatencyThroughputMetricTest extends FlatSpec
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

    doReturn(Set.empty).when(prod1).predecessors
    doReturn(Set(prod1)).when(op1).predecessors

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

    doReturn(Map(op1 -> 1.0)).when(prod1).selectivities
    doReturn(Map(op2 -> 1.0)).when(op1).selectivities
    doReturn(Map(cons1 -> 1.0)).when(op2).selectivities

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
   *
   *         /--> op1--\
   *  prod1 -           -> op3 -> cons1
   *         \--> op2--/
   */
  trait Fixture2 extends CommonFixture {
    val path2 = mock[VertexPath]("path2")
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
    doReturn(Set(prod1)).when(placement).producers
    doReturn(Set(cons1)).when(placement).consumers
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

    doReturn(Iterator(prod1, prod2, op1, op2, op3, cons1)).when(placement).iterator
    doReturn(Set(prod1, prod2)).when(placement).producers
    doReturn(Set(cons1)).when(placement).consumers
  }


  "A LatencyMetricTest" should "calculate the correct latency and throughput for one iteration" in new Fixture1 {
    val calc = LatencyThroughputCalculator(placement)

    calc.update(Generated(prod1,  0.0, 10.0, EventSet(10.0, 10.0,  0.0, Map(prod1 -> 10.0))))
    calc.update(Produced (prod1, 10.0, 11.0, EventSet(10.0, 11.0,  1.0, Map(prod1 -> 10.0))))
    calc.update(Produced (op1,   11.0, 15.0, EventSet(10.0, 15.0,  5.0, Map(prod1 -> 10.0))))
    calc.update(Produced (op2,   15.0, 20.0, EventSet(10.0, 20.0, 10.0, Map(prod1 -> 10.0))))
    calc.update(Consumed (cons1, 20.0, 25.0, EventSet(10.0, 25.0, 15.0, Map(prod1 -> 10.0))))

    val latency = calc.results(LatencyMetric.ID, cons1)
    latency should have size (1)
    latency.head should be (LatencyMetric(cons1, 25.0, 10.0, 15.0))
    
    val throughput = calc.results(ThroughputMetric.ID, cons1)
    throughput should have size (1)
    throughput.head should be (ThroughputMetric(cons1, 0.0, 10.0))

  }

  it should "calculate the right latency and throughput when there are two paths for the same producer" in new Fixture2 {
    val calc = LatencyThroughputCalculator(placement)

    calc update Generated(prod1,  0.0, 10.0, EventSet(10.0, 10.0, 0.0,  prod1 -> 10.0))
    calc update Produced (prod1, 10.0, 11.0, EventSet(10.0, 11.0, 1.0,  prod1 -> 10.0))
    calc update Produced (op1,   11.0, 21.0, EventSet(10.0, 21.0, 11.0, prod1 -> 10.0))
    calc update Produced (op2,   21.0, 31.0, EventSet(10.0, 31.0, 21.0, prod1 -> 10.0))
    calc update Produced (op3,   31.0, 35.0, EventSet( 3.0, 35.0, 25.0, prod1 -> 20.0))
    calc update Consumed (cons1, 35.0, 42.0, EventSet( 3.0, 42.0, 32.0, prod1 -> 20.0))

    val latency = calc.results(LatencyMetric.ID, cons1)
    latency should have size (1)
    latency.head should be (LatencyMetric(cons1, 42.0, 3.0, 32.0))

    val throughput = calc.results(ThroughputMetric.ID, cons1)
    throughput should have size (1)
    throughput.head should be (ThroughputMetric(cons1, 0.0, 10.0))
  }


  it should "calculate the right latency and throughput when there are more than one producer" in new Fixture3 {
    val throughput = LatencyThroughputCalculator(placement)

    throughput update Generated(prod1,  0.0, 10.0, EventSet(10.0, 10.0,  0.0, prod1 -> 10.0))
    throughput update Produced (prod1, 10.0, 20.0, EventSet(10.0, 20.0, 10.0, prod1 -> 10.0))
    throughput update Generated(prod2,  0.0, 20.0, EventSet(20.0, 20.0,  0.0, prod2 -> 20.0))
    throughput update Produced (prod2, 20.0, 30.0, EventSet(20.0, 30.0, 10.0, prod2 -> 20.0))

    throughput update Produced (op1,   30.0, 40.0, EventSet(10.0, 40.0, 30.0, prod1 -> 10.0))
    throughput update Produced (op2,   40.0, 50.0, EventSet(20.0, 50.0, 30.0, prod2 -> 20.0))
    throughput update Produced (op3,   50.0, 55.0, EventSet( 5.0, 55.0, 40.0, prod1 -> 10.0, prod2 -> 20.0))
    throughput update Consumed (cons1, 55.0, 60.0, EventSet( 5.0, 60.0, 45.0, prod1 -> 10.0, prod2 -> 20.0))


    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (1)
    results(0) should be (ThroughputMetric(cons1, 0.0, 30.0))
  }

  it should "correctly split the throughput measurements in seconds" in new Fixture1 {
    val throughput = LatencyThroughputCalculator(placement)
    throughput.init(0.0)

    throughput update Consumed (cons1,   50.0,  100.0, EventSet(10.0,  100.0, 20.0, prod1 -> 10.0))
    throughput update Consumed (cons1,  500.0,  550.0, EventSet(10.0,  550.0, 20.0, prod1 -> 10.0))
    throughput update Consumed (cons1,  900.0,  950.0, EventSet( 5.0,  950.0, 20.0, prod1 ->  5.0))
    throughput update Consumed (cons1, 1100.0, 1150.0, EventSet(10.0, 1150.0, 20.0, prod1 -> 10.0))
    throughput update Consumed (cons1, 5000.0, 5050.0, EventSet( 6.0, 5050.0, 20.0, prod1 ->  6.0))

    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (3)
    results(0) should be (ThroughputMetric(cons1, 0.0, 25.0))
    results(1) should be (ThroughputMetric(cons1, 1.0, 10.0))
    results(2) should be (ThroughputMetric(cons1, 5.0,  6.0))
  }


  it should "consider the init time in calculation" in new Fixture1 {
    val throughput = LatencyThroughputCalculator(placement)
    throughput.init(100.0)

    throughput update Consumed (cons1,  200.0,  300.0, EventSet(10.0,  300.0, 20.0, prod1 -> 10.0))
    throughput update Consumed (cons1, 1000.0, 1050.0, EventSet( 5.0, 1050.0, 20.0, prod1 -> 10.0)) // still the first second
    throughput update Consumed (cons1, 1100.0, 1200.0, EventSet(10.0, 1200.0, 20.0, prod1 -> 10.0))

    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (2)
    results(0) should be (ThroughputMetric(cons1, 0.0, 20.0))
    results(1) should be (ThroughputMetric(cons1, 1.0, 10.0))
  }


  it should "consider the init time in calculation when there are two paths for the same producer" in new Fixture2 {
    val throughput = LatencyThroughputCalculator(placement)
    throughput.init(100.0)

    throughput update Consumed (cons1,  200.0,  300.0, EventSet(10.0,  300.0, 20.0, prod1 -> 10.0))
    throughput update Consumed (cons1, 1000.0, 1050.0, EventSet( 5.0, 1050.0, 20.0, prod1 -> 10.0)) // still the first second
    throughput update Consumed (cons1, 1100.0, 1200.0, EventSet(10.0, 1200.0, 20.0, prod1 -> 10.0))

    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (2)
    results(0) should be (ThroughputMetric(cons1, 0.0, 10.0))
    results(1) should be (ThroughputMetric(cons1, 1.0,  5.0))
  }



  it should "consolidate latency per minute" in new Fixture1 {
    val latency = LatencyThroughputCalculator(placement)
    latency.init(0.0)

    latency update Consumed (cons1,     50.0,    100.0, EventSet(10.0,    100.0, 20.0, prod1 -> 10.0))
    latency update Consumed (cons1,  50000.0,  50500.0, EventSet(10.0,  50500.0, 10.0, prod1 -> 10.0))
    latency update Consumed (cons1,  60000.0,  60050.0, EventSet( 5.0,  60050.0, 20.0, prod1 ->  5.0))
    latency update Consumed (cons1,  61000.0,  61050.0, EventSet(20.0,  61050.0, 10.0, prod1 -> 20.0))
    latency update Consumed (cons1, 190000.0, 190050.0, EventSet(15.0, 190050.0, 20.0, prod1 -> 15.0))


    val results = latency.results(LatencyMetric.ID, cons1)
    results should contain theSameElementsInOrderAs(List(
      LatencyMetric(cons1,    100.0, 10.0, 20.0),
      LatencyMetric(cons1,  50500.0, 10.0, 10.0),
      LatencyMetric(cons1,  60050.0,  5.0, 20.0),
      LatencyMetric(cons1,  61050.0, 20.0, 10.0),
      LatencyMetric(cons1, 190050.0, 15.0, 20.0)
    ))

    val consolidated = latency.consolidateByMinute(LatencyMetric.ID, cons1)
    consolidated should have size (3)
    consolidated(0) should be (15.000 +- 0.0001)
    consolidated(1) should be (12.000 +- 0.0001)
    consolidated(3) should be (20.000 +- 0.0001)
  }


  it should "consolidate throughput per minute" in new Fixture1 {
    val throughput = LatencyThroughputCalculator(placement)
    throughput.init(0.0)

    throughput update Consumed (cons1,     50.0,    100.0, EventSet(10.0,    100.0, 20.0, prod1 -> 10.0))
    throughput update Consumed (cons1,  50000.0,  50500.0, EventSet(10.0,  50500.0, 20.0, prod1 -> 10.0))
    throughput update Consumed (cons1,  60000.0,  60050.0, EventSet( 5.0,  60050.0, 20.0, prod1 ->  5.0))
    throughput update Consumed (cons1, 190000.0, 190050.0, EventSet(15.0, 190050.0, 20.0, prod1 -> 15.0))


    val results = throughput.results(ThroughputMetric.ID, cons1)
    results should have size (4)
    results(0) should be (ThroughputMetric(cons1,   0.0, 10.0))
    results(1) should be (ThroughputMetric(cons1,  50.0, 10.0))
    results(2) should be (ThroughputMetric(cons1,  60.0,  5.0))
    results(3) should be (ThroughputMetric(cons1, 190.0, 15.0))

    val consolidated = throughput.consolidateByMinute(ThroughputMetric.ID, cons1)
    consolidated should have size (3)
    consolidated(0) should be (0.3333 +- 0.0001)
    consolidated(1) should be (0.0833 +- 0.0001)
    consolidated(3) should be (0.2500 +- 0.0001)
  }


  


}



