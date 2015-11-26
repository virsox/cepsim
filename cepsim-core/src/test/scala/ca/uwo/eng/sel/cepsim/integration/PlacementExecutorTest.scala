package ca.uwo.eng.sel.cepsim.integration

import ca.uwo.eng.sel.cepsim._
import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator
import ca.uwo.eng.sel.cepsim.history.{Consumed, Generated, Produced}
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}


/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class PlacementExecutorTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val gen = UniformGenerator(100000)

    val prod1 = EventProducer("p1", 1000, gen)
    val f1 = Operator("f1", 4000)
    val f2 = Operator("f2", 4000)
    val cons1 = EventConsumer("c1", 1000)

    val query1 = Query("q1", Set(prod1, f1, f2, cons1), Set((prod1, f1, 1.0), (f1, f2, 1.0), (f2, cons1, 0.1)))

  }

  "A PlacementExecutor" should "send events through the operator graph" in new Fixture {

    // executor going to use 10 millions instructions (10 ms)
    val executor = PlacementExecutor("c1", Placement(query1, 1), DefaultOpScheduleStrategy.weighted()) //, 0.0)
    executor.init(0.0)

    val h = executor run (10000000, 10.0, 1000)

    prod1.outputQueues(f1) should be(0)
    f1.outputQueues(f2) should be(0)
    f2.outputQueues(cons1) should be(0)
    cons1.outputQueue should be(100)

    // check if history is being correctly logged
    h should have size (5)
    h.toList should contain theSameElementsInOrderAs (List(
      Generated(prod1,  0.0, 10.0, EventSet(1000.0, 10.0,  0.0, prod1 -> 1000.0)),
      Produced (prod1, 10.0, 11.0, EventSet(1000.0, 11.0,  1.0, prod1 -> 1000.0)),
      Produced (f1,    11.0, 15.0, EventSet(1000.0, 15.0,  5.0, prod1 -> 1000.0)),
      Produced (f2,    15.0, 19.0, EventSet(1000.0, 19.0,  9.0, prod1 -> 1000.0)),
      Consumed (cons1, 19.0, 20.0, EventSet( 100.0, 20.0, 10.0, prod1 -> 1000.0))
    ))
  }

  it should "accumulate the number of produced events" in new Fixture {
    val executor = PlacementExecutor("c1", Placement(query1, 1), DefaultOpScheduleStrategy.weighted())//, 0.0)
    executor.init(0.0)

    executor run (10000000, 10.0, 1000)
    executor run (10000000, 20.0, 1000)

    prod1.outputQueues(f1) should be (0)
    f1.outputQueues(f2) should be (0)
    f2.outputQueues(cons1) should be (0)
    cons1.outputQueue should be (200)
  }

  it should "correctly invoke the metric calculation" in new Fixture {

    val calculator = mock[metric.MetricCalculator]
    doReturn(Set("latency")).when(calculator).ids

    val executor = PlacementExecutor("c1", Placement(query1, 1), DefaultOpScheduleStrategy.weighted(), 1, calculator)
    executor.init(0.0)

    executor run(10000000, 10.0, 1000)

    verify(calculator).update(Generated(prod1,  0.0, 10.0, EventSet(1000.0, 10.0,  0.0, prod1 -> 1000.0)))
    verify(calculator).update(Produced (prod1, 10.0, 11.0, EventSet(1000.0, 11.0,  1.0, prod1 -> 1000.0)))
    verify(calculator).update(Produced (f1,    11.0, 15.0, EventSet(1000.0, 15.0,  5.0, prod1 -> 1000.0)))
    verify(calculator).update(Produced (f2,    15.0, 19.0, EventSet(1000.0, 19.0,  9.0, prod1 -> 1000.0)))
    verify(calculator).update(Consumed (cons1, 19.0, 20.0, EventSet( 100.0, 20.0, 10.0, prod1 -> 1000.0)))

  }

  it should "run all queries in the placement" in new Fixture {
    val prod2 = EventProducer("p2", 1000, gen)
    val f3 = Operator("f3", 4000)
    val f4 = Operator("f4", 4000)
    val cons2 = EventConsumer("c2", 1000)
    val query2 = Query("q2", Set(prod2, f3, f4, cons2), Set((prod2, f3, 1.0), (f3, f4, 1.0), (f4, cons2, 0.1)))

    val placement = Placement(query1.vertices ++ query2.vertices, 1)
    val executor = PlacementExecutor("c1", placement, DefaultOpScheduleStrategy.weighted())
    executor.init(0.0)

    val h = executor run (10000000, 10.0, 1000)
    prod1.outputQueues(f1) should be(0)
    f1.outputQueues(f2) should be(0)
    f2.outputQueues(cons1) should be(0)
    cons1.outputQueue should be(50)

    prod2.outputQueues(f3) should be(0)
    f3.outputQueues(f4) should be(0)
    f4.outputQueues(cons2) should be(0)
    cons2.outputQueue should be(50)

    h should have size (10)
    h.toList should contain theSameElementsInOrderAs (List(
      Generated(prod2,  0.0, 10.0, EventSet(1000, 10.0,  0.0, prod2 -> 1000.0)),
      Generated(prod1,  0.0, 10.0, EventSet(1000, 10.0,  0.0, prod1 -> 1000.0)),
      Produced (prod1, 10.0, 10.5, EventSet( 500, 10.5,  0.5, prod1 ->  500.0)),
      Produced (prod2, 10.5, 11.0, EventSet( 500, 11.0,  1.0, prod2 ->  500.0)),
      Produced (f1,    11.0, 13.0, EventSet( 500, 13.0,  3.0, prod1 ->  500.0)),
      Produced (f3,    13.0, 15.0, EventSet( 500, 15.0,  5.0, prod2 ->  500.0)),
      Produced (f2,    15.0, 17.0, EventSet( 500, 17.0,  7.0, prod1 ->  500.0)),
      Produced (f4,    17.0, 19.0, EventSet( 500, 19.0,  9.0, prod2 ->  500.0)),
      Consumed (cons1, 19.0, 19.5, EventSet(  50, 19.5,  9.5, prod1 ->  500.0)),
      Consumed (cons2, 19.5, 20.0, EventSet(  50, 20.0, 10.0, prod2 ->  500.0))
    ))
  }

}


