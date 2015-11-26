package ca.uwo.eng.sel.cepsim.integration

import ca.uwo.eng.sel.cepsim.PlacementExecutor
import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator
import ca.uwo.eng.sel.cepsim.history.{Consumed, Produced, Generated}
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by virso on 2014-08-13.
 */
@RunWith(classOf[JUnitRunner])
class BoundedPlacementExecutorTest extends FlatSpec
  with Matchers {

  trait Fixture {
    val gen = UniformGenerator(100000)

    val prod1 = EventProducer("p1", 1000, gen, true)
    val f1 = Operator("f1", 2500, 1000)
    val f2 = Operator("f2", 8000, 1000)
    val cons1 = EventConsumer("c1", 1000, 1000)

    var query1 = Query("q1", Set(prod1, f1, f2, cons1), Set((prod1, f1, 1.0), (f1, f2, 0.5), (f2, cons1, 0.1)))

    //val vm = Vm("vm1", 1000) // 1 billion instructions per second

  }

  "A PlacementExecutor" should "respect the buffer bounds" in new Fixture {
    val placement = Placement(query1, 1)
    val schedStrategy = DefaultOpScheduleStrategy.uniform()
    var executor = PlacementExecutor("c1", placement, schedStrategy) //, 0.0)
    executor.init(0.0)

    // 10 millions instructions ~ 10 milliseconds
    // 1000 events will be generated per simulation tick
    // each operator will use 2.5 millions instructions
    val h = executor.run(10000000, 10.0, 1000)


    prod1.outputQueues(f1) should be(0)
    f1.outputQueues(f2)    should be(0)

    f2.inputQueues(f1)     should be(187.50 +- 0.01) // f2 will process 312.5 events only (from the total of 500)
    f2.outputQueues(cons1) should be(0.0)
    cons1.inputQueues(f2)  should be(0.00 +- 0.01)    // 0.25 will be accumulated into cons1
    cons1.outputQueue      should be(31.25)

    // check if history is being correctly logged
    h should have size (5)
    h.toList should contain theSameElementsInOrderAs (List(
      Generated(prod1,  0.0, 10.0, EventSet(1000.00, 10.0,  0.0, prod1 -> 1000.00)),
      Produced (prod1, 10.0, 12.5, EventSet(1000.00, 12.5,  2.5, prod1 -> 1000.00)),
      Produced (f1,    12.5, 15.0, EventSet(1000.00, 15.0,  5.0, prod1 -> 1000.00)),
      Produced (f2,    15.0, 17.5, EventSet( 312.50, 17.5,  7.5, prod1 ->  625.00)),
      Consumed (cons1, 17.5, 20.0, EventSet(  31.25, 20.0, 10.0, prod1 ->  625.00))))

    // --------------------------------------------------------------------------
    // SECOND ITERATION
    // --------------------------------------------------------------------------
    executor.run(10000000, 20.0, 1000)

    prod1.outputQueues(f1) should be(0)

    f1.inputQueues(prod1)  should be(0) // f2 buffer can only store more 687.5 events, so 1375 can be processed
    f1.outputQueues(f2)    should be(0)

    f2.inputQueues(f1)     should be(375.0 +- 0.01)
    f2.outputQueues(cons1) should be(0)
    cons1.inputQueues(f2)  should be(0.00  +- 0.01)
    cons1.outputQueue      should be(62.50)

    // --------------------------------------------------------------------------
    // THIRD ITERATION
    // --------------------------------------------------------------------------
    executor.run(10000000, 30.0, 1000)

    prod1.outputQueues(f1) should be(0)

    f1.inputQueues(prod1)  should be(0) // f2 buffer can only store more 625 events, so 1250 can be processed
    f1.outputQueues(f2)    should be(0)

    f2.inputQueues(f1)     should be(562.50 +- 0.01)
    f2.outputQueues(cons1) should be(0)
    cons1.inputQueues(f2)  should be(0.00  +- 0.01)
    cons1.outputQueue      should be(93.75)

    // --------------------------------------------------------------------------
    // FOURTH ITERATION
    // --------------------------------------------------------------------------
    executor.run(10000000, 40.0, 1000)

    prod1.outputQueues(f1) should be(0)

    f1.inputQueues(prod1)  should be(125) // f2 buffer can only store more 437.5 events, so 875 can be processed
    f1.outputQueues(f2)    should be(0)

    f2.inputQueues(f1)     should be(687.50)
    f2.outputQueues(cons1) should be(0)
    cons1.inputQueues(f2)  should be(0.00  +- 0.01)

    cons1.outputQueue      should be(125)

    // --------------------------------------------------------------------------
    // FIFTH ITERATION
    // --------------------------------------------------------------------------
    executor.run(10000000, 50.0, 1000)

    prod1.outputQueues(f1) should be(0) // f1 buffer can only store more 875 events
    prod1.inputQueue       should be(0)
    gen.nonProcessed       should be(125)

    f1.inputQueues(prod1)  should be(375) // f2 buffer can only store more 312.5 events, so 625 can be processed
    f1.outputQueues(f2)    should be(0)

    f2.inputQueues(f1)     should be(687.50)
    f2.outputQueues(cons1) should be(0)
    cons1.inputQueues(f2)  should be(0.00 +- 0.01)
    cons1.outputQueue      should be(156.25)

  }


}
