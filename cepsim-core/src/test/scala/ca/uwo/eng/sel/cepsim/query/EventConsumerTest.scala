package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.Consumed
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

    val op1 = mock[Operator]
    val op2 = mock[Operator]
    when(op1.id) thenReturn("op1")
    when(op2.id) thenReturn("op2")

    val query = mock[Query]

  }

  "An EventConsumer" should "send all events to the output" in new Fixture {
    val cons1 = EventConsumer("c1", 1)
    cons1 addInputQueue op1

    cons1 enqueueIntoInput(op1, 100)
    val simEvent = cons1 run(100, 0, 100)
    simEvent should be (List(Consumed(cons1, 0, 100, 100.0, Map(op1 -> 100.0))))

    cons1.inputQueues(op1) should be (0)
    cons1.outputQueue should be (100)
  }

  it should "consume events from all predecessors" in new Fixture {
    val cons1 = EventConsumer("c1", 1)
    cons1 addInputQueue op1
    cons1 addInputQueue op2

    cons1 enqueueIntoInput(op1, 100)
    cons1 enqueueIntoInput(op2, 50)

    val simEvent = cons1.run(100, 0, 100)(0).asInstanceOf[Consumed]
    simEvent should equal (Consumed(cons1, 0, 100, 100.00, Map(op1 -> 66.666, op2 -> 33.333)))

    cons1.inputQueues(op1) should be (33.333 +- 0.001)
    cons1.inputQueues(op2) should be (16.666 +- 0.001)
    cons1.outputQueue should be (100)
  }

  it should "accumulate partial events" in new Fixture {
    val cons1 = EventConsumer("c1", 3)
    cons1 addInputQueue op1
    cons1 enqueueIntoInput (op1, 100)

    val simEvent = cons1.run(100, 0, 100)(0).asInstanceOf[Consumed]
    simEvent should equal (Consumed(cons1, 0, 100, 33.0, Map(op1 -> 33.333)))
    cons1.inputQueues(op1) should be (66.666 +- 0.001)
    cons1.outputQueue should be (33)

    val simEvent2 = cons1.run(100, 100, 200)(0).asInstanceOf[Consumed]
    simEvent2 should equal (Consumed(cons1, 100, 200, 33.0, Map(op1 -> 33.333)))
    cons1.inputQueues(op1) should be (33.333 +- 0.001)
    cons1.outputQueue should be (66)

    val simEvent3 = cons1.run(100, 200, 300)(0).asInstanceOf[Consumed]
    simEvent3 should equal (Consumed(cons1, 200, 300, 34.0, Map(op1 -> 33.333)))
    cons1.inputQueues(op1) should be (0.000 +- 0.001)
    cons1.outputQueue should be (100)

  }

  it should "consume input events even if no output is generated" in new Fixture {
    val cons1 = EventConsumer("c1", 100)
    cons1 addInputQueue op1
    cons1 enqueueIntoInput (op1, 1)

    val simEvent = cons1.run(50, 0, 50)
    simEvent should be (List(Consumed(cons1, 0, 50, 0, Map(op1 -> 0.5))))
  }


  it should "not generate any SimEvent if no input events are consumed neither output is generated" in new Fixture {
    val cons1 = EventConsumer("c1", 100)
    cons1 addInputQueue op1
    cons1 enqueueIntoInput (op1, 0)

    val simEvent = cons1.run(50, 0, 50)
    simEvent should be (List())
  }

}
