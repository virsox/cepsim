package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.gen.Generator
import ca.uwo.eng.sel.cepsim.metric.History.Entry
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, Operator, EventProducer, Query}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2014-08-02.
 */
@RunWith(classOf[JUnitRunner])
class LatencyMetricTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val gen = mock[Generator]
    val p1 = mock[EventProducer]
    val p2 = mock[EventProducer]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val c1 = mock[EventConsumer]

    doReturn(gen).when(p1).generator
    doReturn(gen).when(p2).generator

    doReturn("p1").when(p1).id
    doReturn("p2").when(p2).id
    doReturn("f1").when(f1).id
    doReturn("f2").when(f2).id
    doReturn("c1").when(c1).id
  }


  "The LatencyMetric" should "calculate the correct value in a simple query" in new Fixture {
    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 0.1)))

    doReturn(50.0).when(gen).average
    doReturn(5).when(c1).outputQueue

    val h = mock[History]
    doReturn(List(Entry("cloudlet1", 0.0,  p1, 50))).when(h).from(p1)
    doReturn(Some(Entry("cloudlet1", 10.0, c1,  5))).when(h).from(c1, 10)

    val latency = LatencyMetric.calculate(q, h, c1, 10)
    latency should be (10)
  }

  it should "calculate the same value in consecutive iterations" in new Fixture {
    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 0.1)))

    doReturn(50.0).when(gen).average
    doReturn(10).when(c1).outputQueue

    val h = mock[History]
    doReturn(List(Entry("cloudlet1", 0.0,  p1, 50), Entry("cloudlet1", 11.0,  p1, 50))).when(h).from(p1)
    doReturn(Some(Entry("cloudlet1", 10.0, c1,  5))).when(h).from(c1, 10)
    doReturn(Some(Entry("cloudlet1", 21.0, c1,  5))).when(h).from(c1, 21)

    var latency = LatencyMetric.calculate(q, h, c1, 10)
    latency should be (10)

    latency = LatencyMetric.calculate(q, h, c1, 21)
    latency should be (10)
  }


  it should "calculate the correct value when the producer history needs to be traversed" in new Fixture {
    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 1.0)))

    doReturn(10.0).when(gen).average
    doReturn(5).when(c1).outputQueue

    val h = mock[History]
    val h1 = Entry("cloudlet1", 0.0,  p1, 10)
    doReturn(List(h1, Entry("cloudlet1", 8.0,  p1, 10), Entry("cloudlet1", 12.0,  p1, 10))).when(h).from(p1)
    doReturn(Some(Entry("cloudlet1", 4.0, f1, 10))).when(h).successor(h1)
    doReturn(Some(Entry("cloudlet1", 15.0, c1,  25))).when(h).from(c1, 14.0)

    val latency = LatencyMetric.calculate(q, h, c1, 14.0)
    latency should be (13)
  }

  
  it should "calculate the correct value when there are more than one producer" in new Fixture {
    val q = Query("q1", Set(p1, p2, f1, f2, c1), Set((p1, f1, 1.0), (p2, f2, 1.0), (f1, c1, 0.5), (f2, c1, 0.1)))

    doReturn(10.0).when(gen).average
    doReturn(12).when(c1).outputQueue

    val h = mock[History]

    val h1 = Entry("cloudlet1",  0.0,  p1, 10)
    val h2 = Entry("cloudlet1", 10.0,  p1, 10)
    val h3 = Entry("cloudlet1", 20.0,  p1, 10)

    val h4 = Entry("cloudlet1",  0.0,  p2, 15)
    val h5 = Entry("cloudlet1", 10.0,  p2, 10)
    val h6 = Entry("cloudlet1", 20.0,  p2, 5)

    doReturn(List(h1, h2, h3)).when(h).from(p1)
    doReturn(List(h4, h5, h6)).when(h).from(p2)
    doReturn(Some(Entry("cloudlet1", 3.0, f1, 10))).when(h).successor(h4)
    doReturn(Some(Entry("cloudlet1", 28.0, c1, 12))).when(h).from(c1, 28)

    val latency = LatencyMetric.calculate(q, h, c1, 28)
    latency should be (26)

  }

}
