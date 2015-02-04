package ca.uwo.eng.sel.cepsim.query

import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by virso on 2014-07-21.
 */
@RunWith(classOf[JUnitRunner])
class OperatorTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val query = mock[Query]
    val p1 = Operator("p1", 10)
    val p2 = Operator("p2", 10)
    val p3 = Operator("p3", 10)
    val p4 = Operator("p4", 10)
    val n1 = Operator("n1", 10)


    def setup(op: Operator, outputSelectivity: Double, predecessors: Operator*) = {
      predecessors.foreach(op.addInputQueue(_))
      op.addOutputQueue(n1, outputSelectivity)
    }

    def enqueue(op: Operator, sizes: Double*) = {
      op.enqueueIntoInput(p1, sizes(0))
      if (sizes.length > 1) op.enqueueIntoInput(p2, sizes(1))
      if (sizes.length > 2) op.enqueueIntoInput(p3, sizes(2))
      if (sizes.length > 3) op.enqueueIntoInput(p4, sizes(3))
    }

    def assertInput(op: Operator, sizes: Double*) = {
      op.inputQueues(p1) should be (sizes(0) +- 0.01)
      if (sizes.length > 1) op.inputQueues(p2) should be (sizes(1) +- 0.01)
      if (sizes.length > 2) op.inputQueues(p3) should be (sizes(2) +- 0.01)
      if (sizes.length > 3) op.inputQueues(p4) should be (sizes(3) +- 0.01)
    }

  }

  "An operator" should "consume all the input queue" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 10)

    op.run(100)

    assertInput(op, 0)
    op.outputQueues(n1) should be (10)
  }

  it should "consume half of the input queue" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 10)

    op.run(50)

    assertInput(op, 5)
    op.outputQueues(n1) should be (5)
  }

  it should "correctly consume the input queue even if it has spare instructions" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 10)

    op.run(1000)
    assertInput(op, 0)
    op.outputQueues(n1) should be (10)
  }

  it should "run even if there are no events in the input" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 1.0, p1)
    enqueue(op, 0)

    op.run(1000)
    assertInput(op, 0)
    op.outputQueues(n1) should be (0)
  }


  "A filter operator" should "correctly apply selectivity" in new Fixture {
    val op = Operator("f1", 10)
    setup(op, 0.1, p1)
    enqueue(op, 10)

    op.run(100)

    assertInput(op, 0)
    op.outputQueues(n1) should be (1)
  }

  "A union operator" should "consume events from both input queues" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2)
    enqueue(op, 10, 10)

    op.run(200)

    assertInput(op, 0, 0)
    op.outputQueues(n1) should be (20)
  }

  it should " partially consume from both input queues" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2)
    enqueue(op, 10, 10)

    op.run(100)

    assertInput(op, 5, 5)
    op.outputQueues(n1) should be (10)
  }


  it should "distribute the processing according to the queue size" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2)
    enqueue(op, 12, 4)

    op.run(120)

    assertInput(op, 3, 1)
    op.outputQueues(n1) should be (12)

  }

  it should "not waste CPU instructions" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2, p3)
    enqueue(op, 10, 10, 10)

    // there are three queues, and it is possible to process 10 events in total
    op.run(100)

    assertInput(op, 6.66, 6.66, 6.66)
    op.outputQueues(n1) should be (10.00)
  }

  it should "not waste CPU instructions again" in new Fixture {
    val op = Operator("u1", 10)
    setup(op, 1.0, p1, p2, p3, p4)
    enqueue(op, 10, 10, 10, 10)

    // there are four queues, and it is possible to process 27 events in total
    op.run(270)

    assertInput(op, 3.25, 3.25, 3.25, 3.25)
    op.outputQueues(n1) should be (27)
  }

  "An aggregate operator" should "consume events from all queues and apply selectivity" in new Fixture {
    val op = Operator("a1", 10)
    setup(op, 0.1, p1, p2, p3, p4)
    enqueue(op, 10, 5, 14, 11) // 0.25 (7.5), 0.125 (3.75), 0.35 (10.5), 0.275 (8.25)

    op.run(300)

    assertInput(op, 2.5, 1.25, 3.5, 2.75)
    op.outputQueues(n1) should be (3)
  }


  "A selective operator" should "accumulate events from previous run" in new Fixture {
    val op = Operator("a1", 10)
    setup(op, 0.05, p1, p2)
    enqueue(op, 5, 5)

    op.run(100)

    assertInput(op, 0, 0)
    op.outputQueues(n1) should be (0.5)

    enqueue(op, 5, 5)
    op.run(100)

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
    op.run(1000)

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
    op.run(50)

    assertInput(op, 5)
    op.outputQueues(n1) should be (0.5)
    op.outputQueues(n2) should be (2.5)

    op.run(50)
    assertInput(op, 0)
    op.outputQueues(n1) should be (1)
    op.outputQueues(n2) should be (5)
  }

  "An operator" should "be able to create more events" in new Fixture {
    val op = Operator("split", 10)
    setup(op, 5.0, p1)

    enqueue(op, 10)
    op.run(100)

    assertInput(op, 0)
    op.outputQueues(n1) should be (50.0)
  }

  // ---------------------------------------------------------------------------------------

  "Any Operator" should "respect the bounds of the successor buffer" in new Fixture {
    val op = Operator("s1", 10)

    op.addInputQueue(p1)
    op.addOutputQueue(n1)

    op.setLimit(n1, 5)

    enqueue(op, 10)
    op.run(100)

    assertInput(op, 5)
    op.outputQueues(n1) should be(5)
  }

  it should "respect the bounds of all successors" in new Fixture {
    val op = Operator("s1", 10)
    val n2 = Operator("n2", 1)

    op.addInputQueue(p1)
    op.addOutputQueue(n1)
    op.addOutputQueue(n2)

    op.setLimit(n1, 5)
    op.setLimit(n2, 3)

    enqueue(op, 10)
    op.run(100)

    assertInput(op, 7)
    op.outputQueues(n1) should be (3)
    op.outputQueues(n2) should be (3)
  }


  it should "respect the bounds of all successors when they have selectivity" in new Fixture {
    val op = Operator("s1", 10)
    val n2 = Operator("n2", 1)

    op.addInputQueue(p1)
    op.addOutputQueue(n1, 0.5)
    op.addOutputQueue(n2, 0.1)

    op.setLimit(n1, 5) // op can process 10 events
    op.setLimit(n2, 3) // op can process 30 events

    enqueue(op, 100)
    op.run(1000)

    assertInput(op, 90)
    op.outputQueues(n1) should be (5)
    op.outputQueues(n2) should be (1)
  }


}
