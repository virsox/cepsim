package ca.uwo.eng.sel.cepsim.query

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2014-07-23.
 */
class EventConsumerTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    import org.mockito.Matchers._

    val cons1 = new EventConsumer("c1", 1)

    val op1 = mock[Operator]
    val op2 = mock[Operator]
    when(op1.id) thenReturn("op1")
    when(op2.id) thenReturn("op2")

    val query = mock[Query]

  }

  "An EventConsumer" should "send all events to the output" in new Fixture {
    cons1 addInputQueue op1

    cons1 enqueueIntoInput(op1, 100)
    cons1 run(100)

    cons1.inputQueues(op1) should be (0)
    cons1.outputQueue should be (100)
  }

  it should "consume events from all predecessors" in new Fixture {
    cons1 addInputQueue op1
    cons1 addInputQueue op2

    cons1 enqueueIntoInput(op1, 100)
    cons1 enqueueIntoInput(op2, 50)
    cons1 run(100)

    cons1.inputQueues(op1) should be (33)
    cons1.inputQueues(op2) should be (17)
    cons1.outputQueue should be (100)
  }

}
