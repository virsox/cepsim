package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.history.Produced
import ca.uwo.eng.sel.cepsim.util.SimEventBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by virso on 2014-07-21.
 */
@RunWith(classOf[JUnitRunner])
class OperatorTest extends FlatSpec
  with Matchers
  with MockitoSugar
  with SimEventBaseTest {

  trait Fixture {
    val query = mock[Query]

    val p1 = mock[EventProducer]("p1")
    val p2 = mock[EventProducer]("p2")
    val p3 = mock[EventProducer]("p3")
    val p4 = mock[EventProducer]("p4")
    val n1 = Operator("n1", 10)


    def setup(op: Operator, outputSelectivity: Double, predecessors: Vertex*) = {
      predecessors.foreach(op.addInputQueue(_))
      op.addOutputQueue(n1, outputSelectivity)
    }

    def enqueue(op: Operator, sizes: Double*) = {
      op.enqueueIntoInput(p1, EventSet(sizes(0), 0, 0, Map(p1 -> sizes(0))))
      if (sizes.length > 1) op.enqueueIntoInput(p2, EventSet(sizes(1), 0, 0, Map(p2 -> sizes(1))))
      if (sizes.length > 2) op.enqueueIntoInput(p3, EventSet(sizes(2), 0, 0, Map(p3 -> sizes(2))))
      if (sizes.length > 3) op.enqueueIntoInput(p4, EventSet(sizes(3), 0, 0, Map(p4 -> sizes(3))))
    }

    def assertInput(op: Operator, sizes: Double*) = {
      op.inputQueues(p1) should equal (sizes(0) +- 0.01)
      if (sizes.length > 1) op.inputQueues(p2) should equal (sizes(1) +- 0.01)
      if (sizes.length > 2) op.inputQueues(p3) should equal (sizes(2) +- 0.01)
      if (sizes.length > 3) op.inputQueues(p4) should equal (sizes(3) +- 0.01)
    }

  }


  "An operator" should "consume all the input queue" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 10)

    val simEvent = op.run(100, 0, 10)
    simEvent should be (List(Produced(op, 0, 10, EventSet(10.0, 10.0, 10.0, p1 -> 10.0))))

    assertInput(op, 0)
    op.outputQueues(n1) should be (10)
  }

  it should "consume half of the input queue" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 10)

    val simEvent = op.run(50, 0, 5)
    simEvent should be (List(Produced(op, 0, 5, EventSet(5.0, 5.0, 5.0, p1 -> 5.0))))

    assertInput(op, 5)
    op.outputQueues(n1) should be (5)
  }

  it should "correctly consume the input queue even if it has spare instructions" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 10)

    val simEvent = op.run(1000, 0, 100)
    simEvent should be (List(Produced(op, 0, 100, EventSet(10.0, 100.0, 100.0, p1 -> 10.0))))

    assertInput(op, 0)
    op.outputQueues(n1) should be (10)
  }

  it should "run even if there are no events in the input" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 0)

    val simEvent = op.run(1000, 0, 100)
    simEvent should be (List.empty)

    assertInput(op, 0)
    op.outputQueues(n1) should be (0)
  }

  it should "calculate the correct latency / throughput in one iteration" in new Fixture {
    val op = Operator("f1", 10)
    op addInputQueue (p1)
    op addOutputQueue(n1, 1.0)

    op.enqueueIntoInput(p1, EventSet(10.0, 200.0, 50.0, p1 -> 1000.0))
    val outputEs = op.run(100.0, 300.0, 500.0)(0).es

    outputEs.size    should be( 10.0)
    outputEs.ts      should be(500.0)
    outputEs.latency should be(350.0)
    outputEs.totals  should be(Map(p1 -> 1000.0))
  }

  it should "correctly enqueue incoming event sets" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 10.0)   // currently in the queue EventSet(10, 0, 0, Map(p1 -> 10))

    op enqueueIntoInput(p1, EventSet(10, 20, 10, p1 -> 10.0))
    // queue is now EventSet(10, 0, 0, Map(p1 -> 10)), EventSet(10, 20, 10, p1 -> 10)

    // first es only
    var outputEs = op.run(100.0, 30, 40)(0).es
    outputEs should be (EventSet(10.0, 40.0, 40.0, p1 -> 10.0))

    // second es
    outputEs = op.run(100.0, 40, 50)(0).es
    outputEs should be (EventSet(10.0, 50.0, 40.0, p1 -> 10.0))
  }


  it should "dequeue partial event sets" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 10.0)

    // queue is now EventSet(10, 0, 0, Map(p1 -> 10)), EventSet(20, 10, 5, p1 -> 20)
    op enqueueIntoInput(p1, EventSet(10, 20, 10, p1 -> 10.0))

    var outputEs = op.run(150.0, 30, 45)(0).es
    outputEs should equal (EventSet(15.0, 45.0, 41.6666, p1 -> 15.0))
  }


  // -----------------------------------------------------------------------------------


  "A filter operator" should "correctly apply selectivity" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 0.1, p1)
    enqueue(op, 10)

    val simEvent = op.run(100, 0 , 10)

    // 10 events have been produced, but only 1 is written to the output queue
    simEvent should be (List(Produced(op, 0, 10, EventSet(10.0, 10.0, 10.0, p1 -> 10.0))))
    assertInput(op, 0)
    op.outputQueues(n1) should be (1)
  }

  "A union operator" should "consume events from both input queues" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2)
    enqueue(op, 10, 10)

    val simEvent = op.run(200, 0, 20)
    simEvent should be (List(Produced(op, 0, 20, EventSet(20.0, 20.0, 20.0, p1 -> 10.0, p2 -> 10.0))))

    assertInput(op, 0, 0)
    op.outputQueues(n1) should be (20)
  }

  it should " partially consume from both input queues" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2)
    enqueue(op, 10, 10)

    val simEvent = op.run(100, 0, 10)
    simEvent should be (List(Produced(op, 0, 10, EventSet(10.0, 10.0, 10.0, p1 -> 5.0, p2 -> 5.0))))

    assertInput(op, 5, 5)
    op.outputQueues(n1) should be (10)
  }

  it should "dequeue partial event sets from both input queues" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2)

    op.enqueueIntoInput(p1, EventSet(10.0, 20.0, 10.0, p1 -> 10.0))
    op.enqueueIntoInput(p2, EventSet(10.0, 10.0,  5.0, p2 -> 10.0))

    var outputEs = op.run(100.0, 30, 40)(0).es
    outputEs should equal (EventSet(10.0, 40.0, 32.50, p1 -> 5.0, p2 -> 5.0))
  }


  it should "distribute the processing according to the queue size" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2)
    enqueue(op, 12, 4)

    val simEvent = op.run(120, 0, 12)
    simEvent should be (List(Produced(op, 0, 12, EventSet(12.0, 12.0, 12.0, p1 -> 9.0, p2 -> 3.0))))

    assertInput(op, 3, 1)
    op.outputQueues(n1) should be (12)

  }

  it should "not waste CPU instructions" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2, p3)
    enqueue(op, 10, 10, 10)

    // there are three queues, and it is possible to process 10 events in total
    val simEvent: Produced = op.run(100, 0, 10)(0).asInstanceOf[Produced]
    simEvent should equal (Produced(op, 0, 10, EventSet(10.0, 10, 10, p1 -> 3.333, p2 -> 3.333, p3 -> 3.333)))

    assertInput(op, 6.66, 6.66, 6.66)
    op.outputQueues(n1) should be (10.00)
  }

  it should "not waste CPU instructions again" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2, p3, p4)
    enqueue(op, 10, 10, 10, 10)

    // there are four queues, and it is possible to process 27 events in total
    val simEvent = op.run(270, 0, 27)
    simEvent should be (List(Produced(op, 0, 27, EventSet(27.00, 27, 27, p1 -> 6.75, p2 -> 6.75, p3 -> 6.75, p4 -> 6.75))))

    assertInput(op, 3.25, 3.25, 3.25, 3.25)
    op.outputQueues(n1) should be (27)
  }

  "An aggregate operator" should "consume events from all queues and apply selectivity" in new Fixture {
    val op = Operator("a1", 10)
    setup(op, 0.1, p1, p2, p3, p4)
    enqueue(op, 10, 5, 14, 11) // 0.25 (7.5), 0.125 (3.75), 0.35 (10.5), 0.275 (8.25)

    val simEvent = op.run(300, 0, 30)
    simEvent should be (List(Produced(op, 0, 30, EventSet(30.00, 30, 30, p1 -> 7.5, p2 -> 3.75, p3 -> 10.5, p4 -> 8.25))))

    assertInput(op, 2.5, 1.25, 3.5, 2.75)
    op.outputQueues(n1) should be (3)
  }


  "A selective operator" should "accumulate events from previous run" in new Fixture {
    val op = Operator("a1", 10)
    setup(op, 0.05, p1, p2)
    enqueue(op, 5, 5)

    val simEvent = op.run(100, 0, 10)
    simEvent should be (List(Produced(op, 0, 10, EventSet(10.00, 10.0, 10.0, p1 -> 5.0, p2 -> 5.0))))

    assertInput(op, 0, 0)
    op.outputQueues(n1) should be (0.5)

    op.enqueueIntoInput(p1, EventSet(5, 10, 0, p1 -> 5.0))
    op.enqueueIntoInput(p2, EventSet(5, 10, 0, p2 -> 5.0))

    val simEvent2 = op.run(100, 10, 20)
    simEvent2 should be (List(Produced(op, 10, 20, EventSet(10.00, 20.0, 10.0, p1 -> 5.0, p2 -> 5.0))))

    assertInput(op, 0, 0)
    op.outputQueues(n1) should be (1)

  }

  // ---------------------------------------------------------------------------------------

  "A Split operator" should "split input events into the outputs" in new Fixture {
    val op = Operator("s1", 10)
    val n2 = Operator("n2", 1)

    op.addInputQueue(p1)
    op.addOutputQueue(n1, 0.5)
    op.addOutputQueue(n2, 0.5)

    enqueue(op, 100)
    val simEvent = op.run(1000, 0, 100)
    simEvent should be (List(Produced(op, 0, 100, EventSet(100.00, 100.0, 100.0, p1 -> 100.0))))

    assertInput(op, 0)
    op.outputQueues(n1) should be (50)
    op.outputQueues(n2) should be (50)
  }

  "A selective Split operator" should "accumulate events from previous run" in new Fixture {
    val op = Operator("s1", 10)
    val n2 = Operator("n2", 1)

    op.addInputQueue(p1)
    op.addOutputQueue(n1, 0.1)
    op.addOutputQueue(n2, 0.5)

    enqueue(op, 10)
    val simEvent = op.run(50, 0, 5)
    simEvent should be (List(Produced(op, 0, 5, EventSet(5.00, 5.0, 5.0, p1 -> 5.0))))

    assertInput(op, 5)
    op.outputQueues(n1) should be (0.5)
    op.outputQueues(n2) should be (2.5)

    val simEvent2 = op.run(50, 5, 10)
    simEvent2 should be (List(Produced(op, 5, 10, EventSet(5.00, 10.0, 10.0, p1 -> 5.0))))

    assertInput(op, 0)
    op.outputQueues(n1) should be (1)
    op.outputQueues(n2) should be (5)
  }

  "An operator" should "be able to create more events" in new Fixture {
    val op = Operator("split", 10)
    setup(op, 5.0, p1)

    enqueue(op, 10)
    val simEvent = op.run(100, 0, 10)
    simEvent should be (List(Produced(op, 0, 10, EventSet(10.00, 10.0, 10.0, p1 -> 10.0))))

    assertInput(op, 0)
    op.outputQueues(n1) should be (50.0)
  }

  // ---------------------------------------------------------------------------------------

  "Any Operator" should "respect the bounds of the successor buffer" in new Fixture {
    val op = Operator("s1", 10)
    setup(op, 1.0, p1)

    op.setLimit(n1, 5)

    enqueue(op, 10)
    val simEvent = op.run(100, 0, 10)
    simEvent should be (List(Produced(op, 0, 10, EventSet(5.0, 10.0, 10.0, p1 -> 5.0))))

    assertInput(op, 5)
    op.outputQueues(n1) should be(5)
  }

  it should "respect the bounds of all successors" in new Fixture {
    val op = Operator("s1", 10)
    setup(op, 1.0, p1)

    val n2 = Operator("n2", 1)
    op.addOutputQueue(n2)

    op.setLimit(n1, 5)
    op.setLimit(n2, 3)

    enqueue(op, 10)
    val simEvent = op.run(100, 0, 10)
    simEvent should be (List(Produced(op, 0, 10, EventSet(3.0, 10.0, 10.0, p1 -> 3.0))))

    assertInput(op, 7)
    op.outputQueues(n1) should be (3)
    op.outputQueues(n2) should be (3)
  }


  it should "respect the bounds of all successors when they have selectivity" in new Fixture {
    val op = Operator("s1", 10)
    setup(op, 0.5, p1)

    val n2 = Operator("n2", 1)
    op.addOutputQueue(n2, 0.1)

    op.setLimit(n1, 5) // op can process 10 events
    op.setLimit(n2, 3) // op can process 30 events

    enqueue(op, 100)
    val simEvent = op.run(1000, 0, 100)
    simEvent should be (List(Produced(op, 0, 100, EventSet(10.0, 100, 100, p1 -> 10.0))))

    assertInput(op, 90)
    op.outputQueues(n1) should be (5)
    op.outputQueues(n2) should be (1)
  }

  it should "respect the bounds of a successor when it is scheduled more than once" in new Fixture {
    val op = Operator("s1", 10)
    setup(op, 1.0, p1)

    op.setLimit(n1, 15)
    enqueue(op, 20)
    var simEvent = op.run(100, 0, 100)
    simEvent should be (List(Produced(op, 0, 100, EventSet(10.0, 100, 100, p1 -> 10.0))))

    simEvent = op.run(100, 100, 200)
    simEvent should be (List(Produced(op, 100, 200, EventSet(5.0, 200, 200, p1 -> 5.0))))

    simEvent = op.run(100, 200, 300)
    simEvent should be (List.empty)
  }


  it should "respect the bounds of all successors when it is scheduled more than once" in new Fixture {
    val op = Operator("s1", 10)
    setup(op, 1.0, p1)

    val n2 = Operator("n2", 1)
    op.addOutputQueue(n2, 1.0)

    op.setLimit(n1, 15)
    op.setLimit(n1, 10)
    enqueue(op, 20)
    var simEvent = op.run(100, 0, 100)
    simEvent should be (List(Produced(op, 0, 100, EventSet(10.0, 100, 100, p1 -> 10.0))))

    simEvent = op.run(100, 100, 200)
    simEvent should be (List.empty)
  }


  it should "respect the bounds of all successors with selectivity when it is scheduled more than once" in new Fixture {
    val op = Operator("s1", 10)
    setup(op, 1.0, p1)

    val n2 = Operator("n2", 1)
    op.addOutputQueue(n2, 0.5)

    op.setLimit(n1, 15)
    op.setLimit(n2,  6)
    enqueue(op, 20)
    var simEvent = op.run(100, 0, 100)
    simEvent should be (List(Produced(op, 0.0, 100.0, EventSet(10.0, 100.0, 100.0, p1 -> 10.0))))

    simEvent = op.run(100, 100, 200)
    simEvent should be (List(Produced(op, 100.0, 200.0, EventSet(2.0, 200.0, 200.0, p1 -> 2.0))))

    simEvent = op.run(100, 200, 300)
    simEvent should be (List.empty)
  }


}
