package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{Query, EventConsumer, Operator, EventProducer}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class DefaultOpScheduleStrategyTest extends FlatSpec
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

  "A DefaultOpScheduleStrategy" should "allocate a proportional amount of instructions for operators in the same query" in new Fixture {
    val placement = mock[Placement]
    doReturn(Set(query1)).when(placement).queries
    doReturn(Set(p1, f1, c1)).when(placement).vertices
    doReturn(Set(p1, f1, c1)).when(placement).vertices(query1)
    doReturn(Iterator(p1, f1, c1)).when(placement).iterator

    val strategy = DefaultOpScheduleStrategy.weighted()

    val ret = strategy.allocate(1000, placement)

    ret.next    should be ((p1, 100.0))
    ret.next    should be ((f1, 800.0))
    ret.next    should be ((c1, 100.0))
    ret.hasNext should be (false)
  }

  it should "distribute instructions equally among queries" in new Fixture {
    val placement = mock[Placement]
    doReturn(Set(query1, query2)).when(placement).queries
    doReturn(Set(p1, p2, f1, f2, c1, c2)).when(placement).vertices
    doReturn(Set(p1, f1, c1)).when(placement).vertices(query1)
    doReturn(Set(p2, f2, c2)).when(placement).vertices(query2)
    doReturn(Iterator(p1, p2, f1, f2, c1, c2)).when(placement).iterator

    val strategy = DefaultOpScheduleStrategy.weighted
    val ret = strategy.allocate(1000, placement)


    ret.next    should be ((p1, 50.0))
    ret.next    should be ((p2, 50.0))
    ret.next    should be ((f1, 400.0))
    ret.next    should be ((f2, 400.0))
    ret.next    should be ((c1, 50.0))
    ret.next    should be ((c2, 50.0))
    ret.hasNext should be (false)
  }

  it should "sum contributions from all queries when an operator is shared" in new Fixture {
    val placement = mock[Placement]
    doReturn(Set(query1, query2)).when(placement).queries
    doReturn(Set(p1, p2, f1, c1, c2)).when(placement).vertices
    doReturn(Set(p1, f1, c1)).when(placement).vertices(query1)
    doReturn(Set(p2, f1, c2)).when(placement).vertices(query2)
    doReturn(Iterator(p1, p2, f1, c1, c2)).when(placement).iterator

    val strategy = DefaultOpScheduleStrategy.weighted()
    val ret = strategy.allocate(1000, placement)


    ret.next    should be ((p1, 50.0))
    ret.next    should be ((p2, 50.0))
    ret.next    should be ((f1, 800.0))
    ret.next    should be ((c1, 50.0))
    ret.next    should be ((c2, 50.0))
    ret.hasNext should be (false)
  }

  "A RRScheduleStrategy" should "allocate the same number of instructions for every operator" in {
    val p1 = mock[EventProducer]
    val p2 = mock[EventProducer]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val c1 = mock[EventConsumer]
    val c2 = mock[EventConsumer]

    val placement = mock[Placement]
    doReturn(Set(p1, p2, f1, f2, c1, c2)).when(placement).vertices
    doReturn(Iterator(p1, p2, f1, f2, c1, c2)).when(placement).iterator

    val strategy = DefaultOpScheduleStrategy.uniform()
    val ret = strategy.allocate(1200, placement)

    ret.next    should be ((p1, 200))
    ret.next    should be ((p2, 200))
    ret.next    should be ((f1, 200))
    ret.next    should be ((f2, 200))
    ret.next    should be ((c1, 200))
    ret.next    should be ((c2, 200))
    ret.hasNext should be (false)
  }

  "A UserDefOpScheduleStrategy" should "respect user weights" in new Fixture {
    val prod1 = mock[EventProducer]
    val split1 = mock[Operator]
    val count1 = mock[Operator]
    val cons1 = mock[EventConsumer]
    doReturn(10.0).when(prod1).ipe
    doReturn(50.0).when(split1).ipe
    doReturn(25.0).when(count1).ipe
    doReturn(10.0).when(cons1).ipe

    val query = mock[Query]
    val placement = mock[Placement]

    doReturn(Set(query1)).when(placement).queries
    doReturn(Set(prod1, split1, count1, cons1)).when(placement).vertices
    doReturn(Set(prod1, split1, count1, cons1)).when(placement).vertices(query1)
    doReturn(Iterator(prod1, split1, count1, cons1)).when(placement).iterator
    val weights = Map((prod1 -> 1.0), (split1 -> 1.0), (count1 -> 5.0), (cons1 -> 5.0))

    val strategy = DefaultOpScheduleStrategy.weighted(weights)
    val ret = strategy.allocate(10000, placement)

    ret.hasNext should be (true)
    var pair = ret.next
    pair._1 should be (prod1)
    pair._2 should be (425.53 +- 0.01)

    ret.hasNext should be (true)
    pair = ret.next
    pair._1 should be (split1)
    pair._2 should be (2127.65 +- 0.01)

    ret.hasNext should be (true)
    pair = ret.next
    pair._1 should be (count1)
    pair._2 should be (5319.14 +- 0.01)

    ret.hasNext should be (true)
    pair = ret.next
    pair._1 should be (cons1)
    pair._2 should be (2127.65 +- 0.01)

    ret.hasNext should be (false)

  }
}

