package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.gen.Generator
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

/**
 * Created by virso on 2014-07-30.
 */
@RunWith(classOf[JUnitRunner])
class ThroughputMetricTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val gen = mock[Generator]
    doReturn(10.0).when(gen).average

    val p1 = mock[EventProducer]//("p1", 1, gen)
    val p2 = mock[EventProducer]
    val s1 = mock[Operator]
    val f1 = mock[Operator]//("f1", 1)
    val f2 = mock[Operator]//("f1", 1)
    val pr1 = mock[Operator]
    val pr2 = mock[Operator]
    val pr3 = mock[Operator]
    val c1 = mock[EventConsumer]//("c1", 1)

    doReturn(gen).when(p1).generator
    doReturn(gen).when(p2).generator
    doReturn("p1").when(p1).id
    doReturn("p2").when(p2).id
    doReturn("s1").when(s1).id
    doReturn("f1").when(f1).id
    doReturn("f2").when(f2).id
    doReturn("pr1").when(pr1).id
    doReturn("pr2").when(pr2).id
    doReturn("pr3").when(pr3).id
    doReturn("c1").when(c1).id
  }

  "The ThroughputMetric" should "give the correct value in a simple query" in new Fixture {

    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 1.0)))
    doReturn(100).when(c1).outputQueue

    val metric = ThroughputMetric.calculate(q, 5 seconds)
    metric should be (20)
  }


  it should "give the same value if the sampling interval is smaller" in new Fixture {
    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 1.0)))
    doReturn(10).when(c1).outputQueue

    val metric = ThroughputMetric.calculate(q, 500 milliseconds)
    metric should be (20)
  }


  it should "consider selectivity" in new Fixture {

    val q = Query("q1", Set(p1, f1, c1),
      Set((p1, f1, 1.0), (f1, f2, 0.1), (f2, c1, 0.1)))
    doReturn(500).when(c1).outputQueue

    val metric = ThroughputMetric.calculate(q, 10 seconds)
    metric shouldBe 5000.0 +- 0.01
  }

  it should "correctly calculate the metric in a query with a split operator" in new Fixture {
    val q = Query("q1", Set(p1, s1, f1, f2, pr1, pr2, c1),
      Set((p1 , s1, 1.0), (s1 , f1, 0.4), (s1, pr2, 0.6), (f1, pr1, 0.1),
          (pr1, f2, 1.0), (pr2, f2, 1.0), (f2, c1 , 0.1)))

    doReturn(10).when(c1).outputQueue
    val metric = ThroughputMetric.calculate(q, 1 second)
    metric shouldBe 156.25 +- 0.01
  }


  it should "correctly calculate the metric in a query with two producers" in new Fixture {
    val q = Query("q1", Set(p1, p2, f1, pr1, pr2, f2, c1),
      Set((p1, f1, 1.0), (p2, pr1, 1.0), (f1, pr2, 0.5), (pr1, f2, 1.0), (pr2, f2, 1.0), (f2, c1, 0.1)))

    doReturn(10).when(c1).outputQueue
    val metric = ThroughputMetric.calculate(q, 10 seconds)
    metric shouldBe 13.333 +- 0.01
  }

  it should "correctly calculate the metric in a query with two producers and a split" in new Fixture {
    val q = Query("q1", Set(p1, p2, s1, pr1, pr2, pr3, f2, c1),
      Set((p1, s1,  1.0), (s1, pr1, 0.6), (s1, pr2, 0.4), (pr1, f2, 1.0), (pr2, f2, 1.0),
          (p2, pr3, 1.0), (pr3, f2, 1.0), (f2, c1, 0.1)))

    doReturn(10).when(c1).outputQueue
    val metric = ThroughputMetric.calculate(q, 10 seconds)
    metric shouldBe 10.0 +- 0.01
  }
}


