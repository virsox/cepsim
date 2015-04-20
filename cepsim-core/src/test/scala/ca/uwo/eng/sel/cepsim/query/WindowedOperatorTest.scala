package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.history.{Produced, WindowAccumulated}
import ca.uwo.eng.sel.cepsim.metric.EventSet
import ca.uwo.eng.sel.cepsim.util.SimEventBaseTest
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
  with MockitoSugar
  with SimEventBaseTest {

  trait Fixture {
    val prod1 = mock[EventProducer]("prod1")

    val f1 = mock[Operator]("f1")
    val f2 = mock[Operator]("f2")
    val f3 = mock[Operator]("f3")
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
    op enqueueIntoInput (f1, EventSet(10, 5.0, 1.0, prod1 -> 10.0))
    op enqueueIntoInput (f2, EventSet(10, 8.0, 2.0, prod1 -> 10.0))
    val simEvent = op run (200, 10.0, 1000.0)

    simEvent should be (List(WindowAccumulated(op, 10, 1000, 0, EventSet(20.0, 6.5, 1.5, prod1 -> 20.0))))
    op.inputQueues (f1) should be (0.0 +- 0.0001)
    op.inputQueues (f2) should be (0.0 +- 0.0001)
    op.outputQueues(f3) should be (0.0 +- 0.0001)

    // second run - end of the first window - these events shouldn't be considered by the function
    op enqueueIntoInput (f1, EventSet(10, 1000.0, 5.0, prod1 -> 10.0))
    op enqueueIntoInput (f2, EventSet(10, 1000.0, 5.0, prod1 -> 10.0))
    val simEvent2 = op run (200, 1010, 2000)

    simEvent2 should be (List(Produced         (op, 1010, 2000, EventSet(20.0, 2000.0, 1995.0, prod1 -> 20.0)),
                              WindowAccumulated(op, 1010, 2000, 0, EventSet(20.0, 1000.0, 5.0, prod1 -> 20.0))))
    op.inputQueues (f1) should be (0.0 +- 0.0001)
    op.inputQueues (f2) should be (0.0 +- 0.0001)
    op.outputQueues(f3) should be (20.0 +- 0.0001)
    op.accumulatedSlot  should be (0)
  }


  it should "accumulate events from consecutive runs" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 1 second, WindowedOperator.identity(), 1000)
    setup(op)

    op.init(0.0, 500)

    // first run - process the events and accumulate
    op enqueueIntoInput (f1, EventSet(10, 5.0, 1.0, prod1 -> 10.0))
    op enqueueIntoInput (f2, EventSet(10, 8.0, 2.0, prod1 -> 10.0))
    var simEvent = op run (200, 10.0, 200.0)
    simEvent should be (List(WindowAccumulated(op, 10, 200.0, 0, EventSet(20.0, 6.5, 1.5, prod1 -> 20.0))))

    op enqueueIntoInput (f1, EventSet(10, 200.0, 1.0, prod1 -> 10.0))
    op enqueueIntoInput (f2, EventSet(10, 100.0, 0.0, prod1 -> 10.0))
    simEvent = op run (200, 210.0, 500.0)
    simEvent should be (List(WindowAccumulated(op, 210, 500.0, 0, EventSet(20.0, 150.0, 0.5, prod1 -> 20.0))))

    op enqueueIntoInput (f1, EventSet(10, 500.0, 1.0, prod1 -> 10.0))
    op enqueueIntoInput (f2, EventSet(10, 500.0, 1.0, prod1 -> 10.0))
    simEvent = op run (200, 510.0, 900.0)
    simEvent should be (List(WindowAccumulated(op, 510, 900.0, 0, EventSet(20.0, 500.0, 1.0, prod1 -> 20.0))))

    // finally generate output
    val produced = op.run(200, 1010.0, 1100.0)(0).asInstanceOf[Produced]
    produced should equal (Produced(op, 1010.0, 1100.0, EventSet(60.0, 1100.0, 882.16666, prod1 -> 60.0)))
  }


  it should "generate output at each advance period" in new Fixture {

    def accTs(i: Int): Double = {
      if (i == 0) 0
      else {
        // i = 1  ->   ((1000 * 20) + (20 * 0)) / 40
        // i = 2  ->   ((2000 * 20) + (40 * 500) / 60
        // ....
        ((i * 1000 * 20) + (i * 20 * accTs(i - 1))) / ((i + 1) * 20)
      }
    }

    val op = new WindowedOperator("w1", 10, 10 seconds, 1 second, WindowedOperator.constant(2), 1000)
    setup(op)

    op.init(0.0, 1000)

    // run 10 times until it reaches the 10 second window
    (0 until 10).foreach((i) => {
      val startTime = (i * 1000)
      val endTime   = (i * 1000) + 200

      op enqueueIntoInput (f1, EventSet(10.0, startTime, 0, prod1 -> 10.0))
      op enqueueIntoInput (f2, EventSet(10.0, startTime, 0, prod1 -> 10.0))
      val simEvent = op.run(200, startTime, endTime)

      op.inputQueues (f1) should be (0.0 +- 0.0001)
      op.inputQueues (f2) should be (0.0 +- 0.0001)
      op.outputQueues(f3) should be (i * 2.0 +- 0.0001)
      op.accumulatedSlot  should be (i)

      if (i == 0) {
        simEvent should be (List(WindowAccumulated(op, startTime, endTime, i, EventSet(20.0, startTime, 0.0, prod1 -> 20.0))))
      } else {
        simEvent should be (List(Produced         (op, startTime, endTime, EventSet(2.0, endTime, endTime - accTs(i - 1), prod1 -> 20.0)),
                                 WindowAccumulated(op, startTime, endTime, i, EventSet(20.0, startTime, 0.0, prod1 -> 20.0))))
      }
    })

    val simEvent = op.run(200, 10000, 11000)
    simEvent should be (List(Produced(op, 10000, 11000, EventSet(2, 11000, 11000 - accTs(9), prod1 -> 20.0))))

    op.inputQueues (f1) should be ( 0.0 +- 0.0001)
    op.inputQueues (f2) should be ( 0.0 +- 0.0001)
    op.outputQueues(f3) should be (20.0 +- 0.0001)
    op.accumulatedSlot  should be (0)
  }

  it should "correctly accumulate events on window slots" in new Fixture {

    def accTs(i: Int): Double = {
      if (i == 0) 0
      else {
        // i = 1  ->   ((1000 * 20) + (20 * 0)) / 40
        // i = 2  ->   ((2000 * 20) + (40 * 500) / 60
        // ....
        ((i * 100 * 20) + (i * 20 * accTs(i - 1))) / ((i + 1) * 20)
      }
    }

    val op = new WindowedOperator("w1", 10, 1 second, 100 milliseconds, WindowedOperator.identity(), 1000)
    setup(op)

    op.init(0.0, 100)

    // run 10 times
    (0 until 10).foreach((i) => {
      val startTime = (i * 100)
      val endTime   = (i * 100) + 100

      op enqueueIntoInput (f1, EventSet(10.0, startTime, 0, prod1 -> 10.0))
      op enqueueIntoInput (f2, EventSet(10.0, startTime, 0, prod1 -> 10.0))

      val simEvent = op.run(200, startTime, endTime)

      op.inputQueues (f1) should be (  0.0 +- 0.0001)
      op.inputQueues (f2) should be (  0.0 +- 0.0001)
      op.outputQueues(f3) should be (i * 20.0 +- 0.0001)
      op.dequeueFromOutput(f3, i * 20.00)

      if (i == 0) {
        simEvent should be (List(WindowAccumulated(op, startTime, endTime, i, EventSet(20.0, startTime, 0.0, prod1 -> 20.0))))
      } else {
        simEvent should be (List(Produced         (op, startTime, endTime, EventSet(i * 20, endTime, endTime - accTs(i - 1), prod1 -> 20.0)),
                                 WindowAccumulated(op, startTime, endTime, i, EventSet(20.0, startTime, 0.0, prod1 -> 20.0))))
      }

    })

    // these enqueued events are accumulated in the [1000, 1100[ window
    op enqueueIntoInput (f1, EventSet(5.0, 1000.0, 0, prod1 -> 5.0))
    op enqueueIntoInput (f2, EventSet(5.0, 1000.0, 0, prod1 -> 5.0))

    val simEvent = op.run(200, 1005, 1100)
    simEvent should be (List(Produced         (op, 1005, 1100, EventSet(200, 1100, 1100 - accTs(9), prod1 -> 20.0)),
                             WindowAccumulated(op, 1005, 1100, 0, EventSet(10, 1000, 0, prod1 -> 10.0))))
    op.inputQueues (f1) should be (  0.0 +- 0.0001)
    op.inputQueues (f2) should be (  0.0 +- 0.0001)
    op.outputQueues(f3) should be (200.0 +- 0.0001)
  }

  it should "take the start time into consideration" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 1 second, WindowedOperator.constant(1), 1000)
    setup(op)

    op.init(200, 500)

    // first run - process the events and accumulate
    op enqueueIntoInput (f1, EventSet(10.0, 500.0, 0, prod1 -> 10.0))
    op enqueueIntoInput (f2, EventSet(10.0, 500.0, 0, prod1 -> 10.0))
    val simEvent = op run (200, 500, 1000)

    simEvent should be (List(WindowAccumulated(op, 500, 1000, 0, EventSet(20.0, 500.0, 0.0, prod1 -> 20.0))))
    op.outputQueues(f3) should be (0.0 +- 0.0001)

    // second run - 1 second hasn't elapsed yet because the operator started its processing at time 200ms
    val simEvent2 = op run (200, 1000, 1500)
    simEvent2 should have size (0)
    op.outputQueues(f3) should be (0.0 +- 0.0001)

    // third run - now it does
    val simEvent3 = op run (200, 1500, 2000)
    simEvent3 should be (List(Produced   (op, 1500, 2000, EventSet(1.0, 2000.0, 1500.0, prod1 -> 20.0))))
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
    op enqueueIntoInput (f1, EventSet(10, 1100.0, 5.0, prod1 -> 10.0))
    op enqueueIntoInput (f2, EventSet(10, 1100.0, 5.0, prod1 -> 10.0))
    val simEvent2 = op.run(200, 1101, 1200)
    simEvent2 should be (List(WindowAccumulated(op, 1101, 1200, 1, EventSet(20.0, 1100.0, 5.0, prod1 -> 20.0))))
    op.outputQueues(f3) should be (0.0)


    val simEvent3 = op.run(200, 1200, 1300)
    simEvent3 should be (List(Produced(op, 1200, 1300, EventSet(20.0, 1300.0, 205.0, prod1 -> 20.0))))
    op.outputQueues(f3) should be (20.0 +- 0.001)

  }

  it should "skip more than one slot if needed" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 100 milliseconds, WindowedOperator.identity(), 1000)
    setup(op)

    op.init(0.0, 100)

    // run 10 times until it reaches the 1 second window
    (0 until 10).foreach((i) => {
      val startTime = i * 100
      val endTime   = (i + 1) * 100

      op enqueueIntoInput (f1, EventSet(10.0, startTime, 0.0, prod1 -> 10.0))
      op enqueueIntoInput (f2, EventSet(10.0, startTime, 0.0, prod1 -> 10.0))
      op.run(200, i * 100, (i + 1) * 100)
    })

    // 180 + 160 + 140 + 120 + 100 + 80 + 60 + 40 + 20
    op.outputQueues(f3) should be (900.0 +- 0.0001)
    op.dequeueFromOutput(f3, 900.00)

    val simEvent = op.run(200, 1450, 1550)

    // it should execute 5 windows -
    // (0 -> 1000) = 200, (100 -> 1100) = 180, (200 -> 1200) = 160, (300 -> 1300) = 140, (400 -> 1400) = 120
    // totals is zero from the second Produced because all events had already been taken into account
    simEvent should be (List(Produced(op, 1450, 1550, EventSet(200, 1550, 1100.0, prod1 -> 20.0)),
                             Produced(op, 1450, 1550, EventSet(180, 1550, 1050.0, prod1 ->  0.0)),
                             Produced(op, 1450, 1550, EventSet(160, 1550, 1000.0, prod1 ->  0.0)),
                             Produced(op, 1450, 1550, EventSet(140, 1550,  950.0, prod1 ->  0.0)),
                             Produced(op, 1450, 1550, EventSet(120, 1550,  900.0, prod1 ->  0.0))))

    op.outputQueues(f3) should be (800.0 +- 0.0001)
    op.accumulatedSlot  should be (4)
  }


  it should "respect the bounds of the successor buffer" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 1 second, WindowedOperator.identity(), 1024)
    setup(op)

    op.init(0.0, 1000)

    // first run - process the events and accumulate
    op enqueueIntoInput (f1, EventSet(1000, 0.0, 0.0, prod1 -> 1000.0))
    op enqueueIntoInput (f2, EventSet(1000, 0.0, 0.0, prod1 -> 1000.0))
    var simEvent = op run (20000, 10.0, 1000.0)
    op setLimit (f3, 1000)

    // second run - it can generate only 1000 ouputs, because f3 has a full buffer
    // although, the predecessors still need to be processed and accumulated
    op enqueueIntoInput (f1, EventSet(1000, 1000.0, 0.0, prod1 -> 1000.0))
    op enqueueIntoInput (f2, EventSet(1000, 1000.0, 0.0, prod1 -> 1000.0))
    simEvent = op.run(20000, 1000, 1100)
    simEvent should be (List(Produced         (op, 1000, 1100, EventSet(1000, 1100.0, 1100.0, prod1 -> 1000.0)),
                             WindowAccumulated(op, 1000, 1100, 0, EventSet(2000, 1000.0, 0.0,  prod1 -> 2000.0))))
    op.outputQueues(f3) should be (1000.0 +- 0.0001)
    op.dequeueFromOutput(f3, 1000.0)

    // third run - still generate output (1000 events from the previous window)
    simEvent = op.run(20000, 1100, 1200)
    simEvent should be (List(Produced(op, 1100, 1200, EventSet(1000, 1200.0, 1200.0, prod1 -> 1000.0))))
    op.outputQueues(f3) should be (1000.0 +- 0.0001)
    op.dequeueFromOutput(f3, 1000.0)
  }


  it should "respect the bounds of the successor buffer when there are remaining events " +
            "and the window is closing" in new Fixture {

    val op = new WindowedOperator("w1", 10, 1 second, 1 second, WindowedOperator.identity(), 1024)
    setup(op)

    op.init(0.0, 1000)

    // first run - process the events and accumulate
    op enqueueIntoInput (f1, EventSet(750.0, 0.0, 0.0, prod1 -> 750.0))
    op enqueueIntoInput (f2, EventSet(750.0, 0.0, 0.0, prod1 -> 750.0))
    var simEvent = op run (20000, 10.0, 1000.0)
    op setLimit (f3, 1000)

    // second run - it can generate only 1000 ouputs, because f3 has a full buffer
    // therefore, 500 events form the first window will not be sent to the successor
    op enqueueIntoInput (f1, EventSet(500.0, 1000.0, 0.0, prod1 -> 500.0))
    op enqueueIntoInput (f2, EventSet(500.0, 1000.0, 0.0, prod1 -> 500.0))
    simEvent = op.run(20000, 1000, 2000)
    simEvent should be (List(Produced         (op, 1000, 2000, EventSet(1000, 2000.0, 2000.0, prod1 -> 1000.0)),
                             WindowAccumulated(op, 1000, 2000, 0, EventSet(1000.0, 1000.0, 0.0, prod1 -> 1000.0))))
    op.outputQueues(f3) should be (1000.0 +- 0.0001)
    op.dequeueFromOutput(f3, 1000.0)

    // third run - needs to send the remaining 500 operators from the first window
    // plus 500 from the second window
    simEvent = op.run(20000, 2000, 3000)
    simEvent should be (List(Produced(op, 2000, 3000, EventSet(500, 3000.0, 3000.0, prod1 -> 500.0)),
                             Produced(op, 2000, 3000, EventSet(500, 3000.0, 2000.0, prod1 -> 500.0))))
    op.outputQueues(f3) should be (1000.0 +- 0.0001)
    op.dequeueFromOutput(f3, 1000.0)
  }


  it should "hold events from many slots if the sucessor buffer is full" in new Fixture {
    val op = new WindowedOperator("w1", 10, 1 second, 100 milliseconds, WindowedOperator.identity(), 1000)
    setup(op)

    op.init(0.0, 100)

    // run 10 times until it reaches the 1 second window
    (0 until 10).foreach((i) => {
      val startTime = i * 100
      val endTime = i * 100 + 100

      op enqueueIntoInput (f1, EventSet(10, startTime, 0.0, prod1 -> 10.0))
      op enqueueIntoInput (f2, EventSet(10, startTime, 0.0, prod1 -> 10.0))
      op.run(200, startTime, endTime)
    })
    op.dequeueFromOutput(f3, 900.00)

    op.setLimit(f3, 300)
    var simEvent = op.run(200, 1450, 1550)

    // it should execute 5 windows -
    // (0 -> 1000) = 200, (100 -> 1100) = 180, (200 -> 1200) = 160, (300 -> 1300) = 140, (400 -> 1400) = 120
    // however, only the first window and part of the second are output
    simEvent should be (List(Produced(op, 1450, 1550, EventSet(200, 1550, 1100, prod1 -> 20.0)),
                             Produced(op, 1450, 1550, EventSet(100, 1550, 1050, prod1 ->  0.0))))
    op.outputQueues(f3) should be (300.0 +- 0.0001)
    op.dequeueFromOutput(f3, 300.0)

    // this will produce 100 more events because the (500 -> 1500) window is closing, but they
    // are not sent to the successors because the operator is still catching up with late events
    simEvent = op.run(200, 1550, 1650)
    simEvent should be (List(Produced(op, 1550, 1650, EventSet( 80, 1650.0, 1150.0, prod1 -> 0.0)),
                             Produced(op, 1550, 1650, EventSet(160, 1650.0, 1100.0, prod1 -> 0.0)),
                             Produced(op, 1550, 1650, EventSet( 60, 1650.0, 1050.0, prod1 -> 0.0))))
    op.outputQueues(f3) should be (300.0 +- 0.0001)
    op.dequeueFromOutput(f3, 300.0)


    // 80 more events because the (600 -> 1600) window is closing
    simEvent = op.run(200, 1650, 1750)
    simEvent should be (List(Produced(op, 1650, 1750, EventSet( 80, 1750.0, 1150.0, prod1 -> 0.0)),
                             Produced(op, 1650, 1750, EventSet(120, 1750.0, 1100.0, prod1 -> 0.0)),
                             Produced(op, 1650, 1750, EventSet(100, 1750.0, 1050.0, prod1 -> 0.0))))
    op.outputQueues(f3) should be (300.0 +- 0.0001)
    op.dequeueFromOutput(f3, 300.0)


    // 60 more events because the (700 -> 1700) window is closing
    simEvent = op.run(200, 1750, 1850)
    simEvent should be (List(Produced(op, 1750, 1850, EventSet(80, 1850.0, 1100.0, prod1 -> 0.0)),
                             Produced(op, 1750, 1850, EventSet(60, 1850.0, 1050.0, prod1 -> 0.0))))
    op.outputQueues(f3) should be (140.0 +- 0.0001)

  }


}
