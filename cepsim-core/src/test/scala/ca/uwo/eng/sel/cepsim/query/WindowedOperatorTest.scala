package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.{WindowProduced, WindowAccumulated}
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
    val op = new WindowedOperator("w1", 10, 1 second, 1 second, WindowedOperator.identity(), 1000)
    setup(op)

    op.init(0.0, 500)

    // first run - process the events and accumulate
    op enqueueIntoInput (f1, 10)
    op enqueueIntoInput (f2, 10)
    val simEvent = op run (200, 10.0, 1000.0)

    simEvent should be (List(WindowAccumulated(op, 10, 1000, 0, Map(f1 -> 10.0, f2 -> 10.0))))
    op.inputQueues (f1) should be (0.0 +- 0.0001)
    op.inputQueues (f2) should be (0.0 +- 0.0001)
    op.outputQueues(f3) should be (0.0 +- 0.0001)

    // second run - end of the first window - these events shouldn't be considered by the function
    op enqueueIntoInput (f1, 10)
    op enqueueIntoInput (f2, 10)
    val simEvent2 = op run (200, 1010, 2000)

    simEvent2 should be (List(WindowProduced   (op, 1010, 2000, 20.0, 0),
                              WindowAccumulated(op, 1010, 2000, 0, Map(f1 -> 10.0, f2 -> 10.0))))
    op.inputQueues (f1) should be (0.0 +- 0.0001)
    op.inputQueues (f2) should be (0.0 +- 0.0001)
    op.outputQueues(f3) should be (20.0 +- 0.0001)
    op.accumulatedSlot  should be (0)
  }


  it should "generate output at each advance period" in new Fixture {
    val op = new WindowedOperator("w1", 10, 10 seconds, 1 second, WindowedOperator.constant(2), 1000)
    setup(op)

    op.init(0.0, 1000)

    // run 10 times until it reaches the 10 second window
    (0 until 10).foreach((i) => {
      val startTime = (i * 1000)
      val endTime   = (i * 1000) + 200

      op enqueueIntoInput (f1, 10)
      op enqueueIntoInput (f2, 10)
      val simEvent = op.run(200, startTime, endTime)

      op.inputQueues (f1) should be (0.0 +- 0.0001)
      op.inputQueues (f2) should be (0.0 +- 0.0001)
      op.outputQueues(f3) should be (i * 2.0 +- 0.0001)
      op.accumulatedSlot  should be (i)

      if (i == 0) {
        simEvent should be (List(WindowAccumulated(op, startTime, endTime, i, Map(f1 -> 10.0, f2 -> 10.0))))
      } else {
        simEvent should be (List(WindowProduced   (op, startTime, endTime, 2, i - 1),
                                 WindowAccumulated(op, startTime, endTime, i, Map(f1 -> 10.0, f2 -> 10.0))))
      }
    })

    val simEvent = op.run(200, 10000, 11000)

    simEvent should be (List(WindowProduced   (op, 10000, 11000, 2, 9)))
    op.inputQueues (f1) should be ( 0.0 +- 0.0001)
    op.inputQueues (f2) should be ( 0.0 +- 0.0001)
    op.outputQueues(f3) should be (20.0 +- 0.0001)
    op.accumulatedSlot  should be (0)
  }

  it should "correctly accumulate events on window slots" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 100 milliseconds, WindowedOperator.identity(), 1000)
    setup(op)

    op.init(0.0, 100)

    // run 10 times
    (0 until 10).foreach((i) => {
      val startTime = (i * 100)
      val endTime   = (i * 100) + 100

      op enqueueIntoInput (f1, 10)
      op enqueueIntoInput (f2, 10)
      val simEvent = op.run(200, startTime, endTime)

      op.inputQueues (f1) should be (  0.0 +- 0.0001)
      op.inputQueues (f2) should be (  0.0 +- 0.0001)
      op.outputQueues(f3) should be (i * 20.0 +- 0.0001)
      op.dequeueFromOutput((f3, i * 20.00))

      if (i == 0) {
        simEvent should be (List(WindowAccumulated(op, startTime, endTime, i, Map(f1 -> 10.0, f2 -> 10.0))))
      } else {
        simEvent should be (List(WindowProduced   (op, startTime, endTime, i * 20, i - 1),
                                 WindowAccumulated(op, startTime, endTime, i, Map(f1 -> 10.0, f2 -> 10.0))))
      }

    })

    // these enqueued events are accumulated in the [1000, 1100[ window
    op enqueueIntoInput (f1, 5)
    op enqueueIntoInput (f2, 5)
    val simEvent = op.run(200, 1005, 1100)

    simEvent should be (List(WindowProduced   (op, 1005, 1100, 200, 9),
                             WindowAccumulated(op, 1005, 1100, 0, Map(f1 -> 5.0, f2 -> 5.0))))
    op.inputQueues (f1) should be (  0.0 +- 0.0001)
    op.inputQueues (f2) should be (  0.0 +- 0.0001)
    op.outputQueues(f3) should be (200.0 +- 0.0001)
  }

  it should "take the start time into consideration" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 1 second, WindowedOperator.constant(1), 1000)
    setup(op)

    op.init(200, 500)

    // first run - process the events and accumulate
    op enqueueIntoInput (f1, 10)
    op enqueueIntoInput (f2, 10)
    val simEvent = op run (200, 500, 1000)

    simEvent should be (List(WindowAccumulated(op, 500, 1000, 0, Map(f1 -> 10.0, f2 -> 10.0))))
    op.outputQueues(f3) should be (0.0 +- 0.0001)

    // second run - 1 second hasn't elapsed yet because the operator started its processing at time 200ms
    val simEvent2 = op run (200, 1000, 1500)
    simEvent2 should be (List())
    op.outputQueues(f3) should be (0.0 +- 0.0001)

    // third run - now it does
    val simEvent3 = op run (200, 1500, 2000)
    simEvent3 should be (List(WindowProduced   (op, 1500, 2000, 1, 0)))
    op.outputQueues(f3) should be (1.0 +- 0.0001)

  }

  it should "not emit anything if there is no event accumulated" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 100 milliseconds, WindowedOperator.identity(), 1000)
    setup(op)

    op.init(0.0, 100)

    val simEvent = op.run(200, 1000, 1100)
    simEvent should have size (0)
    op.outputQueues(f3) should be (0.0)

    // these events are accumulated into a new window
    op enqueueIntoInput (f1, 10)
    op enqueueIntoInput (f2, 10)
    val simEvent2 = op.run(200, 1101, 1200)
    simEvent2 should be (List(WindowAccumulated(op, 1101, 1200, 1, Map(f1 -> 10.0, f2 -> 10.0))))
    op.outputQueues(f3) should be (0.0)


    val simEvent3 = op.run(200, 1200, 1300)
    simEvent3 should be (List(WindowProduced(op, 1200, 1300, 20.0, 1)))
    op.outputQueues(f3) should be (20.0 +- 0.001)

  }



  it should "skip more than one slot if needed" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 100 milliseconds, WindowedOperator.identity(), 1000)
    setup(op)

    op.init(0.0, 100)

    // run 10 times until it reaches the 1 second window
    (0 until 10).foreach((i) => {
      op enqueueIntoInput (f1, 10)
      op enqueueIntoInput (f2, 10)
      op.run(200, i * 100)
    })

    // 180 + 160 + 140 + 120 + 100 + 80 + 60 + 40 + 20
    op.outputQueues(f3) should be (900.0 +- 0.0001)
    op.dequeueFromOutput((f3, 900.00))

    val simEvent = op.run(200, 1450, 1550)

    // it should execute 5 windows -
    // (0 -> 1000) = 200, (100 -> 1100) = 180, (200 -> 1200) = 160, (300 -> 1300) = 140, (400 -> 1400) = 120,
    simEvent should be (List(WindowProduced(op, 1450, 1550, 200, 9),
                             WindowProduced(op, 1450, 1550, 180, 0),
                             WindowProduced(op, 1450, 1550, 160, 1),
                             WindowProduced(op, 1450, 1550, 140, 2),
                             WindowProduced(op, 1450, 1550, 120, 3)))

    op.outputQueues(f3) should be (800.0 +- 0.0001)
    op.accumulatedSlot  should be (4)
  }


}
