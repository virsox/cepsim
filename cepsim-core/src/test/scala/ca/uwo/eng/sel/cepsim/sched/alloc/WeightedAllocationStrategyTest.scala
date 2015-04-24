package ca.uwo.eng.sel.cepsim.sched.alloc

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{Query, EventConsumer, Operator, EventProducer}
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2015-04-23.
 */
class WeightedAllocationStrategyTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val p1 = mock[EventProducer]
    val p2 = mock[EventProducer]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val c1 = mock[EventConsumer]
    val c2 = mock[EventConsumer]
    doReturn(10.0).when(p1).ipe
    doReturn(10.0).when(p2).ipe
    doReturn(80.0).when(f1).ipe
    doReturn(80.0).when(f2).ipe
    doReturn(10.0).when(c1).ipe
    doReturn(10.0).when(c2).ipe

    val query1 = mock[Query]
    val query2 = mock[Query]
  }

  "A WeightedAllocationStrategy" should "allocate instructions according to operators' ipe" in new Fixture {
    val placement = mock[Placement]
    doReturn(Set(query1)).when(placement).queries
    doReturn(Set(p1, f1, c1)).when(placement).vertices
    doReturn(Set(p1, f1, c1)).when(placement).vertices(query1)
    doReturn(Iterator(p1, f1, c1)).when(placement).iterator

    val strategy = WeightedAllocationStrategy.apply()
    val ret = strategy.instructionsPerOperator(1000, placement)

    ret should have size (3)
    ret should be (Map(p1 -> 100.0, f1 -> 800.0, c1 -> 100.0))
  }


  it should "distribute instructions equally among queries" in new Fixture {
    val placement = mock[Placement]
    doReturn(Set(query1, query2)).when(placement).queries
    doReturn(Set(p1, p2, f1, f2, c1, c2)).when(placement).vertices
    doReturn(Set(p1, f1, c1)).when(placement).vertices(query1)
    doReturn(Set(p2, f2, c2)).when(placement).vertices(query2)
    doReturn(Iterator(p1, p2, f1, f2, c1, c2)).when(placement).iterator

    val strategy = WeightedAllocationStrategy.apply()
    val ret = strategy.instructionsPerOperator(1000, placement)

    ret should have size (6)
    ret should be (Map(p1 -> 50.0, f1 -> 400.0, c1 -> 50.0,
                       p2 -> 50.0, f2 -> 400.0, c2 -> 50.0))
  }

  it should "sum contributions from all queries when an operator is shared" in new Fixture {
    val placement = mock[Placement]
    doReturn(Set(query1, query2)).when(placement).queries
    doReturn(Set(p1, p2, f1, c1, c2)).when(placement).vertices
    doReturn(Set(p1, f1, c1)).when(placement).vertices(query1)
    doReturn(Set(p2, f1, c2)).when(placement).vertices(query2)
    doReturn(Iterator(p1, p2, f1, c1, c2)).when(placement).iterator

    val strategy = WeightedAllocationStrategy.apply()
    val ret = strategy.instructionsPerOperator(1000, placement)

    ret should have size (5)
    ret should be (Map(p1 -> 50.0, f1 -> 800.0, c1 -> 50.0,
                       p2 -> 50.0, c2 -> 50.0))
  }

  
  it should "respected user defined weights" in new Fixture {
    doReturn(10.0).when(p1).ipe
    doReturn(50.0).when(f1).ipe
    doReturn(25.0).when(f2).ipe
    doReturn(10.0).when(c1).ipe

    val placement = mock[Placement]

    doReturn(Set(query1)).when(placement).queries
    doReturn(Set(p1, f1, f2, c1)).when(placement).vertices
    doReturn(Set(p1, f1, f2, c1)).when(placement).vertices(query1)
    doReturn(Iterator(p1, f1, f2, c1)).when(placement).iterator

    val weights = Map((p1 -> 1.0), (f1 -> 1.0), (f2 -> 5.0), (c1 -> 5.0))
    val strategy = WeightedAllocationStrategy.apply(weights)
    val ret = strategy.instructionsPerOperator(10000, placement)

    ret should have size (4)
    ret(p1) should be (425.53 +- 0.01)
    ret(f1) should be (2127.65 +- 0.01)
    ret(f2) should be (5319.14 +- 0.01)
    ret(c1) should be (2127.65 +- 0.01)
  }


}
