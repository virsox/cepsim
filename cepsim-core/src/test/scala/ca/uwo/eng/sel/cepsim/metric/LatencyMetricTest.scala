package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.gen.Generator
import ca.uwo.eng.sel.cepsim.metric.History.{Entry, Processed}
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

    var h = History()
    h = h.logProcessed("cloudlet1",  0.0, p1, 50)
    h = h.logProcessed("cloudlet1", 10.0, c1, 5)

    val latency = LatencyMetric.calculate(q, h, c1, 10)
    latency should be (10)
  }

  it should "calculate the same value in consecutive iterations" in new Fixture {
    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 0.1)))
    doReturn(50.0).when(gen).average

    var h = History()
    h = h.logProcessed("cloudlet1",  0.0, p1, 50)
    h = h.logProcessed("cloudlet1", 10.0, c1, 5)
    h = h.logProcessed("cloudlet1", 11.0, p1, 50)
    h = h.logProcessed("cloudlet1", 21.0, c1, 5)


    var latency = LatencyMetric.calculate(q, h, c1, 10)
    latency should be (10)

    latency = LatencyMetric.calculate(q, h, c1, 21)
    latency should be (10)
  }

  it should "calculate the correct value when an producer is partially processed" in new Fixture {
    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 1.0)))
    doReturn(10.0).when(gen).average

    var h = History()
    h = h.logProcessed("cloudlet1",  0.0, p1, 10)
    h = h.logProcessed("cloudlet1", 10.0, p1, 10)
    h = h.logProcessed("cloudlet1", 20.0, p1, 10)
    h = h.logProcessed("cloudlet1", 25.0, c1, 15)  // 25
    h = h.logProcessed("cloudlet1", 30.0, c1, 15)  // 20

    val latency = LatencyMetric.calculate(q, h, c1)
    latency should be (22.5 +- 0.001)
  }

  it should "calculate the correct value when selectivity is greater than one" in new Fixture {
    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 5.0)))
    doReturn(10.0).when(gen).average

    var h = History()
    h = h.logProcessed("cloudlet1",  0.0, p1, 10)
    h = h.logProcessed("cloudlet1", 10.0, f1, 10)
    h = h.logProcessed("cloudlet1", 20.0, c1, 10)  // 20
    h = h.logProcessed("cloudlet1", 30.0, c1, 10)  // 30
    h = h.logProcessed("cloudlet1", 40.0, c1, 10)  // 40
    h = h.logProcessed("cloudlet1", 50.0, c1, 10)  // 50
    h = h.logProcessed("cloudlet1", 60.0, c1, 10)  // 60

    val latency = LatencyMetric.calculate(q, h, c1)
    latency should be (40.0 +- 0.001)
  }

  it should "calculate the correct value when there are more than one producer" in new Fixture {
    val q = Query("q1", Set(p1, p2, f1, f2, c1), Set((p1, f1, 1.0), (p2, f2, 1.0), (f1, c1, 0.5), (f2, c1, 0.1)))
    doReturn(10.0).when(gen).average

    var h = History()
    h = h.logProcessed("cloudlet1",  0.0, p1, 10)
    h = h.logProcessed("cloudlet1",  0.0, p2, 15)
    h = h.logProcessed("cloudlet1",  3.0, f1, 10)
    h = h.logProcessed("cloudlet1", 10.0, p1, 10)
    h = h.logProcessed("cloudlet1", 10.0, p2, 10)
    h = h.logProcessed("cloudlet1", 20.0, p1, 10)
    h = h.logProcessed("cloudlet1", 20.0, p2, 5)
    h = h.logProcessed("cloudlet1", 28.0, c1, 12)

    val latency = LatencyMetric.calculate(q, h, c1, 28)
    latency should be (28)
  }

  it should "calculate the correct consumer average in a query" in new Fixture {
    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 0.1)))
    doReturn(50.0).when(gen).average

    var h = History()
    h = h.logProcessed("cloudlet1",  0.0, p1, 50)
    h = h.logProcessed("cloudlet1", 10.0, c1,  5)
    h = h.logProcessed("cloudlet1", 11.0, p1, 50)
    h = h.logProcessed("cloudlet1", 20.0, c1,  0)
    h = h.logProcessed("cloudlet1", 21.0, p1, 50)
    h = h.logProcessed("cloudlet1", 30.0, c1, 10)

    val latency = LatencyMetric.calculate(q, h, c1)
    latency should be (14.5)    
  }
  
  
  it should "calculate the correct total average in a query with one consumer" in new Fixture {
    val q = Query("q1", Set(p1, f1, c1), Set((p1, f1, 1.0), (f1, c1, 0.1)))
    doReturn(50.0).when(gen).average

    var h = History()
    h = h.logProcessed("cloudlet1", 0.0,  p1, 50)
    h = h.logProcessed("cloudlet1", 10.0, c1,  5)
    
    val latency = LatencyMetric.calculate(q, h)
    latency should be (10)    
  }
  

   it should "calculate the correct total average in a query more than one consumer" in new Fixture {
     val c2 = mock[EventConsumer]
     val q = Query("q1", Set(p1, f1, c1, c2),
                   Set((p1, f1, 1.0), (f1, c1, 0.1), (f1, c2, 0.1)))
        
    doReturn(50.0).when(gen).average

    var h = History()
    h = h.logProcessed("cloudlet1", 0.0,  p1, 50)
    h = h.logProcessed("cloudlet1", 10.0, c1,  5)
    h = h.logProcessed("cloudlet1", 20.0, c2,  5)
    
    val latency = LatencyMetric.calculate(q, h)
    latency should be (15.0 +- 0.001)
  }




}
