package ca.uwo.eng.sel.cepsim.query

import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

/**
 * Created by virso on 14-12-03.
 */
@RunWith(classOf[JUnitRunner])
class WindowedOperatorTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val f3 = mock[Operator]
    doReturn("f1").when(f1).id
    doReturn("f2").when(f2).id
    doReturn("f3").when(f3).id

    def setup(op: WindowedOperator) = {
      op addInputQueue(f1)
      op addInputQueue(f2)
      op addOutputQueue(f3)
    }


  }


  "A WindowedOperator" should "generate an output only after the windows has elapsed" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 1 second, WindowedOperator.constant(2), 1000)
    setup(op)

    op.init(0.0, 500)

    // first run - process the events and accumulate
    op enqueueIntoInput (f1, 10)
    op enqueueIntoInput (f2, 10)
    op run (200, 500)

    op.inputQueues (f1) should be (0.0 +- 0.0001)
    op.inputQueues (f2) should be (0.0 +- 0.0001)
    op.outputQueues(f3) should be (0.0 +- 0.0001)

    // second run - end of the first window
    op enqueueIntoInput (f1, 10)
    op enqueueIntoInput (f2, 10)
    op run (200, 1000)

    op.inputQueues (f1) should be (0.0 +- 0.0001)
    op.inputQueues (f2) should be (0.0 +- 0.0001)
    op.outputQueues(f3) should be (2.0 +- 0.0001)

  }


  it should "generate output at each advance period" in new Fixture {
    val op = new WindowedOperator("w1", 10, 10 seconds, 1 second, WindowedOperator.constant(2), 1000)
    setup(op)

    op.init(0.0, 1000)

    // run 10 times until it reaches the 10 second window
    (1 to 10).foreach((i) => {
      op enqueueIntoInput (f1, 10)
      op enqueueIntoInput (f2, 10)
      op.run(200, i * 1000)
    })
    op.inputQueues (f1) should be (0.0 +- 0.0001)
    op.inputQueues (f2) should be (0.0 +- 0.0001)
    op.outputQueues(f3) should be (2.0 +- 0.0001)

    op.run(200, 11000)
    op.inputQueues (f1) should be (0.0 +- 0.0001)
    op.inputQueues (f2) should be (0.0 +- 0.0001)
    op.outputQueues(f3) should be (4.0 +- 0.0001)

    op.run(200, 12000)
    op.inputQueues (f1) should be (0.0 +- 0.0001)
    op.inputQueues (f2) should be (0.0 +- 0.0001)
    op.outputQueues(f3) should be (6.0 +- 0.0001)
  }


  it should "take the start time into consideration" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 1 second, WindowedOperator.constant(1), 1000)
    setup(op)

    op.init(200, 500)

    // first run - process the events and accumulate
    op enqueueIntoInput (f1, 10)
    op enqueueIntoInput (f2, 10)
    op run (200, 500)
    op.outputQueues(f3) should be (0.0 +- 0.0001)

    // second run - 1 second hasn't elapsed yet because the operator started its processing at time 200ms
    op run (200, 1000)
    op.outputQueues(f3) should be (0.0 +- 0.0001)

    // third run - now it does
    op run (200, 1500)
    op.outputQueues(f3) should be (1.0 +- 0.0001)

  }

  it should "correctly execute the aggregation function" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 100 milliseconds, WindowedOperator.identity(), 1000)
    setup(op)

    op.init(0.0, 100)

    // run 10 times until it reaches the 1 second window
    (1 to 10).foreach((i) => {
      op enqueueIntoInput (f1, 10)
      op enqueueIntoInput (f2, 10)
      op.run(200, i * 100)
    })
    op.inputQueues (f1) should be (  0.0 +- 0.0001)
    op.inputQueues (f2) should be (  0.0 +- 0.0001)
    op.outputQueues(f3) should be (200.0 +- 0.0001)

    op enqueueIntoInput (f1, 10)
    op enqueueIntoInput (f2, 10)
    op.run(200, 11000)
    op.inputQueues (f1) should be (  0.0 +- 0.0001)
    op.inputQueues (f2) should be (  0.0 +- 0.0001)
    op.outputQueues(f3) should be (220.0 +- 0.0001)



  }

}
