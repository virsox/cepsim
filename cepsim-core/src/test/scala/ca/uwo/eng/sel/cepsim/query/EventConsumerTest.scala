package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.Consumed
import ca.uwo.eng.sel.cepsim.metric.EventSet
import ca.uwo.eng.sel.cepsim.util.SimEventBaseTest
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class EventConsumerTest extends FlatSpec
  with Matchers
  with MockitoSugar
  with SimEventBaseTest {

  trait Fixture {

    val prod = mock[EventProducer]("prod")
    val op1 = mock[Operator]("op1")
    val op2 = mock[Operator]("op2")
    when(op1.id) thenReturn("op1")
    when(op2.id) thenReturn("op2")

    val query = mock[Query]
  }


  "An EventConsumer" should "send all events to the output" in new Fixture {
    val cons1 = EventConsumer("c1", 1)
    cons1 addInputQueue op1

    cons1 enqueueIntoInput(op1, EventSet(100.0, 500.0, 200.0, prod -> 100.0))
    val simEvent = cons1 run(100, 900.0, 1000.0)
    simEvent should be (List(Consumed(cons1, 900.0, 1000, EventSet(100.0, 1000.0, 700.0, prod -> 100.0))))

    cons1.inputQueues(op1) should be (0)
    cons1.outputQueue should be (100)
  }

  it should "consume events from all predecessors" in new Fixture {
    val cons1 = EventConsumer("c1", 1)
    cons1 addInputQueue op1
    cons1 addInputQueue op2

    cons1 enqueueIntoInput(op1, EventSet(100.0, 500.0, 100.0, prod -> 100.0))
    cons1 enqueueIntoInput(op2, EventSet( 50.0, 500.0, 100.0, prod ->  50.0))

    val simEvent = cons1.run(100, 1000, 1100)(0).asInstanceOf[Consumed]
    simEvent should equal (Consumed(cons1, 1000, 1100, EventSet(100.00, 1100.0, 700.0, prod -> 100.0)))

    cons1.inputQueues(op1) should be (33.333 +- 0.001)
    cons1.inputQueues(op2) should be (16.666 +- 0.001)
    cons1.outputQueue should be (100)
  }

  it should "accumulate partial events" in new Fixture {
    val cons1 = EventConsumer("c1", 3)
    cons1 addInputQueue op1
    cons1 enqueueIntoInput (op1, EventSet(100, 1000, 0, prod -> 100.0))

    val simEvent = cons1.run(100, 1000, 1100)(0).asInstanceOf[Consumed]
    simEvent should equal (Consumed(cons1, 1000, 1100, EventSet(33.0, 1100.0, 100.0, prod -> 33.0)))
    cons1.inputQueues(op1) should be (66.666 +- 0.001)
    cons1.outputQueue should be (33)

    val simEvent2 = cons1.run(100, 1100, 1200)(0).asInstanceOf[Consumed]
    simEvent2 should equal (Consumed(cons1, 1100, 1200, EventSet(33.0, 1200.0, 200.0, prod -> 33.0)))
    cons1.inputQueues(op1) should be (33.333 +- 0.001)
    cons1.outputQueue should be (66)

    val simEvent3 = cons1.run(100, 1200, 1300)(0).asInstanceOf[Consumed]
    simEvent3 should equal (Consumed(cons1, 1200, 1300, EventSet(34.0, 1300.0, 300.0, prod -> 34.0)))
    cons1.inputQueues(op1) should be (0.000 +- 0.001)
    cons1.outputQueue should be (100)

  }

  it should "consume input events even if no output is generated" in new Fixture {
    val cons1 = EventConsumer("c1", 100)
    cons1 addInputQueue op1
    cons1 enqueueIntoInput (op1, EventSet(1, 0, 0, prod -> 1.0))

    var simEvent = cons1.run(50, 0, 50)
    simEvent should have size (0)
    cons1.inputQueues(op1) should be (0.5 +- 0.001)

    simEvent = cons1.run(50, 50, 100)
    simEvent should have size (1)

    cons1.inputQueues(op1) should be (0.0 +- 0.001)
    cons1.outputQueue should be (1.0)
    simEvent(0) should be (Consumed(cons1, 50, 100, EventSet(1.0, 100.0, 100.0, prod -> 1.0)))

  }


  it should "not generate any SimEvent if no input events are consumed neither output is generated" in new Fixture {
    val cons1 = EventConsumer("c1", 100)
    cons1 addInputQueue op1
    cons1 enqueueIntoInput (op1, EventSet(0, 0, 0, prod -> 0.0))

    val simEvent = cons1.run(50, 0, 50)
    simEvent should have size (0)
  }

}
