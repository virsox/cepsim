package ca.uwo.eng.sel.cepsim.query

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2014-07-30.
 */
@RunWith(classOf[JUnitRunner])
class JoinOperatorTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val p1 = Operator("p1", 1.0)
    val p2 = Operator("p2", 1.0)
    val c1 = Operator("c1", 1.0)
  }

  "A Join operator" should "match events coming from two inputs" in new Fixture {
    val join = JoinOperator("j1", 1, 1.0)

    join addInputQueue(p1)
    join addInputQueue(p2)
    join addOutputQueue(c1)

    join enqueueIntoInput(p1, 10)
    join enqueueIntoInput(p2, 10)


    join run(20)
    join inputQueues(p1) should be (0)
    join inputQueues(p2) should be (0)
    join outputQueues(c1) should be (100)
  }

  it should "match events coming from all inputs and apply reduction" in new Fixture {

    val join = JoinOperator("j1", 1, 0.01)
    val p3 = Operator("p3", 1)

    join addInputQueue(p1)
    join addInputQueue(p2)
    join addInputQueue(p3)

    join addOutputQueue(c1)

    join enqueueIntoInput(p1, 10)
    join enqueueIntoInput(p2, 10)
    join enqueueIntoInput(p3, 10)
    join run(30)

    join.inputQueues should contain theSameElementsAs Set((p1, 0), (p2, 0) ,(p3, 0))
    join outputQueues c1 should be (10)


  }

}
