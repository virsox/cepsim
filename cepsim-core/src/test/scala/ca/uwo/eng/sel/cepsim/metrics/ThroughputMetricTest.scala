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



  "A ThroughputMetricCalculator" should "calculate the right throughput for one iteration" in new Fixture1 {
    val throughput = ThroughputMetric.calculator(placement)

    throughput update Produced (prod1, 10.0, 10.0)
    throughput update Processed(prod1, 11.0, 10.0, Map.empty)
    throughput update Processed(op1,   21.0, 10.0, Map(prod1 -> 10.0))
    throughput update Processed(op2,   22.0,  1.0, Map(op1   ->  1.0))
    throughput update Consumed (cons1, 23.0,  1.0, Map(op2   ->  1.0))

    val results = throughput.results(cons1)
    results should have size (1)

    results(0) should be (ThroughputMetric(cons1, 23.0, 10.0))

  }

  it should "calculate the right throughput for two iterations" in new Fixture1 {
    val throughput = ThroughputMetric.calculator(placement)

    throughput update Produced (prod1, 10.0, 30.0)
    throughput update Processed(prod1, 11.0, 30.0, Map.empty)
    throughput update Processed(op1,   21.0, 30.0, Map(prod1 -> 30.0))
    throughput update Processed(op2,   24.0,  3.0, Map(op1   ->  3.0))
    throughput update Consumed (cons1, 27.0,  3.0, Map(op2   ->  3.0))

    throughput update Produced (prod1, 30.0, 10.0)
    throughput update Processed(prod1, 31.0, 10.0, Map.empty)
    throughput update Processed(op1,   41.0, 10.0, Map(prod1 -> 10.0))
    throughput update Processed(op2,   44.0,  1.0, Map(op1   ->  1.0))
    throughput update Consumed (cons1, 47.0,  1.0, Map(op2   ->  1.0))

    val results = throughput.results(cons1)
    results should have size (2)

    results(0) should be (ThroughputMetric(cons1, 27.0, 30.0))
    results(1) should be (ThroughputMetric(cons1, 47.0, 40.0))
  }


  it should "calculate the right throughput when there are two paths for the same producer" in new Fixture2 {
    val throughput = ThroughputMetric.calculator(placement)

    throughput update Produced (prod1, 10.0, 10.0)
    throughput update Processed(prod1, 11.0, 10.0, Map.empty)
    throughput update Processed(op1,   21.0, 10.0, Map(prod1 -> 10.0))
    throughput update Processed(op2,   31.0, 10.0, Map(prod1 -> 10.0))
    throughput update Processed(op3,   31.0,  3.0, Map(op1 -> 1.0, op2 -> 2.0))
    throughput update Consumed(cons1, 42.0,  3.0, Map(op3 -> 3.0))


    val results = throughput.results(cons1)
    results should have size (1)

    results(0) should be (ThroughputMetric(cons1, 42.0, 10.0))
  }

  it should "calculate the right throughput when there are more than one producer" in new Fixture3 {
    val throughput = ThroughputMetric.calculator(placement)

    throughput update Produced (prod1, 10.0, 10.0)
    throughput update Processed(prod1, 20.0, 10.0, Map.empty)
    throughput update Produced (prod2, 20.0, 20.0)
    throughput update Processed(prod2, 30.0, 20.0, Map.empty)

    throughput update Processed(op1,   40.0, 10.0, Map(prod1 -> 10.0))
    throughput update Processed(op2,   50.0, 10.0, Map(prod2 -> 10.0))
    throughput update Processed(op3,   55.0,  5.0, Map(op1 -> 1.0, op2 -> 4.0))
    throughput update Consumed(cons1,  60.0,  5.0, Map(op3 -> 5.0))


    val results = throughput.results(cons1)
    results should have size (1)
    results(0) should be (ThroughputMetric(cons1, 60.0, 30.0))
  }

}
