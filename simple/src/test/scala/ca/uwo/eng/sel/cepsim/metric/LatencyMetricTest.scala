package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.gen.Generator
import ca.uwo.eng.sel.cepsim.metric.History.Entry
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, Operator, EventProducer, Query}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2014-08-02.
 */
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
    val q = Query(Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 0.1)))

    doReturn(50.0).when(gen).average
    doReturn(5).when(c1).outputQueue

    val h = mock[History]
    doReturn(List(Entry("cloudlet1", 0.0,  p1, 50))).when(h).from(p1)
    doReturn(Entry("cloudlet1", 10.0, c1,  5)).when(h).from("cloudlet1", c1)

    val latency = LatencyMetric.calculate(q, h, "cloudlet1", c1)
    latency should be (10)
  }


  it should "calculate the correct value when the producer history needs to be traversed" in new Fixture {
    val q = Query(Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 0.2)))

    doReturn(10.0).when(gen).average
    doReturn(5).when(c1).outputQueue

    val h = mock[History]
    val h1 = Entry("cloudlet1", 0.0,  p1, 10)
    doReturn(List(h1, Entry("cloudlet2", 8.0,  p1, 10),
                  Entry("cloudlet3", 12.0,  p1, 10))).when(h).from(p1)
    doReturn(Entry("cloudlet1", 4.0, f1, 10)).when(h).successor(h1)
    doReturn(Entry("cloudlet3", 15.0, c1,  25)).when(h).from("cloudlet3", c1)

    val latency = LatencyMetric.calculate(q, h, "cloudlet3", c1)
    latency should be (13)
  }

  it should "calculate the correct value when there are more than one producer" in new Fixture {
    val q = Query(Set(p1, p2, f1, f2, c1), Set((p1, f1, 1.0), (p2, f2, 1.0), (f1, c1, 0.5), (f2, c1, 0.1)))

    doReturn(10.0).when(gen).average
    doReturn(12).when(c1).outputQueue

    val h = mock[History]

    val h1 = Entry("cloudlet1",  0.0,  p1, 10)
    val h2 = Entry("cloudlet2", 10.0,  p1, 10)
    val h3 = Entry("cloudlet3", 20.0,  p1, 10)

    val h4 = Entry("cloudlet1",  0.0,  p1, 15)
    val h5 = Entry("cloudlet2", 10.0,  p1, 10)
    val h6 = Entry("cloudlet3", 20.0,  p1, 5)

    doReturn(List(h1, h2, h3)).when(h).from(p1)
    doReturn(List(h4, h5, h6)).when(h).from(p2)
    doReturn(Entry("cloudlet1", 3.0, f1, 10)).when(h).successor(h4)

    doReturn(Entry("cloudlet3", 28.0, c1, 12)).when(h).from("cloudlet3", c1)

    val latency = LatencyMetric.calculate(q, h, "cloudlet3", c1)
    latency should be (26)

  }

}
