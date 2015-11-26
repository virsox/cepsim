package ca.uwo.eng.sel.cepsim.integration

import ca.uwo.eng.sel.cepsim.PlacementExecutor
import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator
import ca.uwo.eng.sel.cepsim.history.{Consumed, Generated, Produced}
import ca.uwo.eng.sel.cepsim.network.NetworkInterface
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
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
class RemotePlacementExecutorTest extends FlatSpec
  with Matchers
  with MockitoSugar {


  trait Fixture {
    val gen = UniformGenerator(100000)

    val prod1 = EventProducer("p1", 1000, gen)
    val f1 = Operator("f1", 4000)
    val f2 = Operator("f2", 4000)
    val f3 = Operator("f3", 4000)
    val cons1 = EventConsumer("c1", 1000)
    val cons2 = EventConsumer("c2", 1000)

    val query1 = Query("q1", Set(prod1, f1, f2, f3, cons1, cons2),
      Set((prod1, f1, 1.0), (f1, f2, 1.0), (f2, f3, 0.1), (f3, cons2, 0.5), (f2, cons1, 0.1)))

    val placement1 = Placement(Set(prod1, f1, f2, cons1), 1)
    val placement2 = Placement(Set[Vertex](f3, cons2), 2)

  }

  "A PlacementExecutor" should "run operators in the placement only" in new Fixture {
    val network = mock[NetworkInterface]
    val executor1 = PlacementExecutor("c1", placement1, DefaultOpScheduleStrategy.weighted(), 1, network)
    executor1.init(0.0)

    val h = executor1 run (10000000, 10.0, 1000)

    prod1.outputQueues(f1) should be(0)
    f1.outputQueues(f2) should be(0)
    f2.outputQueues(f3) should be(0)
    f2.outputQueues(cons1) should be(0)
    cons1.outputQueue should be(100)

    // check if history is being correctly logged
    h should have size (5)
    h.toList should be (List(
      Generated(prod1,  0.0, 10.0, EventSet(1000, 10.0,  0.0, prod1 -> 1000.0)),
      Produced (prod1, 10.0, 11.0, EventSet(1000, 11.0,  1.0, prod1 -> 1000.0)),
      Produced (f1,    11.0, 15.0, EventSet(1000, 15.0,  5.0, prod1 -> 1000.0)),
      Produced (f2,    15.0, 19.0, EventSet(1000, 19.0,  9.0, prod1 -> 1000.0)),
      Consumed (cons1, 19.0, 20.0, EventSet( 100, 20.0, 10.0, prod1 -> 1000.0))
    ))

    // check if network interface has been invoked
    verify(network).sendMessage(19.0, f2, f3, EventSet(100, 19.0,  9.0, prod1 -> 1000.0))
  }

  it should "correctly exchange events with others executors" in new Fixture {
    val executor1 = PlacementExecutor("c1", placement1, DefaultOpScheduleStrategy.weighted(), 1)
    val executor2 = PlacementExecutor("c2", placement2, DefaultOpScheduleStrategy.weighted(), 1)

    val network = new NetworkInterface {
      override def sendMessage(timestamp: Double, orig: OutputVertex, dest: InputVertex, es: EventSet): Unit = {
        executor2.enqueue(timestamp + 1.0, orig, dest, es)
      }
    }

    executor1.networkInterface = network
    executor2.networkInterface = network
    executor1.init(0.0)
    executor2.init(0.0)

    val h1 = executor1 run (10000000, 10.0, 1000)
    var h2 = executor2 run (10000000, 10.0, 1000)
    h2 should have size (0)

    // in the second run, events from the executor1 should have been enqueued
    h2 = executor2 run (10000000, 20.0, 1000)
    h2 should have size (2)
    h2.toList should be (List(
      Produced (f3,    20.0, 28.0, EventSet(100, 28.0, 18.0, prod1 -> 1000.0)),
      Consumed (cons2, 28.0, 30.0, EventSet( 50, 30.0, 20.0, prod1 -> 1000.0))
    ))

  }



}
