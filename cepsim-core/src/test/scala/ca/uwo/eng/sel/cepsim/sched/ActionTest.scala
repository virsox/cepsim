package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.query.{EventProducer, Operator, Vertex}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2015-04-24.
 */
@RunWith(classOf[JUnitRunner])
class ActionTest  extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val prod = mock[EventProducer]("prod")
    val v1 = mock[Operator]("v1")
    val v2 = mock[Operator]("v2")
  }

  "An Action" should "have temporal relationship with others" in new Fixture {
    val a1 = ExecuteAction(v1, 10.0, 20.0, 100000)
    val a2 = ExecuteAction(v1, 30.0, 40.0, 100000)

    a1.before(a2) should be (true)
    a1.after (a2) should be (false)

    a2.before(a1) should be (false)
    a2.after (a1) should be (true)
  }

  it should "have temporal relationships with instantaneous actions" in new Fixture {
    val a1 = ExecuteAction(v1, 10.0, 20.0, 100000)
    val a2 = EnqueueAction(v1, v2, 30.0, EventSet(35.0, 15.0, 5.0, prod -> 35.0))

    a1.before(a2) should be (true)
    a2.after (a1) should be (true)
  }

  it should "include the start time but should not include the end time" in new Fixture {
    val a1 = ExecuteAction(v1, 10.0, 20.0, 100000)
    val a2 = ExecuteAction(v1, 20.0, 25.0, 100000)

    a1.before(a2) should be (true)
    a1.after (a2) should be (false)
    a2.before(a1) should be (false)
    a2.after (a1) should be (true)
  }


  it should "not have before / after relationships if it overlaps with other action" in new Fixture {
    val a1 = ExecuteAction(v1, 10.0, 20.0, 100000)
    val a2 = ExecuteAction(v1, 15.0, 25.0, 100000)

    a1.before(a2) should be (false)
    a2.before(a1) should be (false)
  }

  it should "correctly implement include relationship" in new Fixture {
    val a1 = ExecuteAction(v1, 10.0, 20.0, 100000)
    val a2 = ExecuteAction(v1, 15.0, 18.0, 100000)

    a1.include(a2) should be (true)
    a2.include(a1) should be (false)
  }

  it should "correctly implement include relationship when the other action is instantaneous" in new Fixture {
    val a1 = ExecuteAction(v1, 10.0, 20.0, 100000)
    val a2 = EnqueueAction(v1, v2, 15.0, EventSet(100.0, 11.0, 1.0, prod -> 100.0))

    a1.include(a2) should be (true)
    a2.include(a1) should be (false)
  }

  it should "be split into two actions" in new Fixture {
    val a1 = ExecuteAction(v1, 10.0, 20.0, 100000)

    val(r1, r2) = a1.splitAt(15.0)

    r1 should be (ExecuteAction(v1, 10.0, 15.0, 50000))
    r2 should be (ExecuteAction(v1, 15.0, 20.0, 50000))
  }


}
