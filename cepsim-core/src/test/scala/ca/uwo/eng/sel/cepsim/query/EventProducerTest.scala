package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.gen.Generator
import ca.uwo.eng.sel.cepsim.history.{Produced, Generated}
import ca.uwo.eng.sel.cepsim.metric.EventSet
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2014-07-23.
 */
@RunWith(classOf[JUnitRunner])
class EventProducerTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val generator = mock[Generator]
    val value: Int = 10000
    doReturn(100.0).when(generator).generate(anyDouble(), anyInt())


    val prod = EventProducer("p1", 1, generator)
    val query = mock[Query]
    val n1 = mock[Operator]
    prod addOutputQueue (n1)
  }

  "An EventProducer" should "generate events and put in the input queue" in new Fixture {
    val result = prod.generate(0, 1000)
    result should be (Generated(prod, 0, 1000, EventSet(100, 1000, 0, prod -> 100.0)))

    verify(generator).generate(org.mockito.Matchers.eq(1000.0), anyInt())
    prod.inputQueue should be (100)
  }

  it should "process events and put them in the output queue" in new Fixture {
    val result1 = prod.generate(0, 1000)
    result1 should be (Generated(prod, 0, 1000, EventSet(100, 1000, 0, prod -> 100.0)))

    val result2 = prod.run(100, 1000, 1100)
    result2    should have size (1)
    result2(0) should be (new Produced(prod, 1000, 1100, EventSet(100, 1100, 100, prod -> 100.0)))

    verify(generator).generate(org.mockito.Matchers.eq(1000.0), anyInt())
    prod.inputQueue should be (0)
    prod.outputQueues(n1) should be (100)
  }

  it should "generate events and put half of them in the output queue" in new Fixture {
    val result1 = prod.generate(0, 500)
    result1 should be (Generated(prod, 0, 500, EventSet(100, 500, 0, prod -> 100.0)))

    val result2 = prod.run(50, 500, 550)
    result2    should have size (1)
    result2(0) should be (new Produced(prod, 500, 550, EventSet(50, 550, 50, prod -> 50.0)))

    verify(generator).generate(org.mockito.Matchers.eq(500.0), anyInt())
    prod.inputQueue should be (50)
    prod.outputQueues(n1) should be (50)
  }

  it should "partially process input events" in new Fixture {
    val prod2 = EventProducer("p2", 3, generator)
    prod2.addOutputQueue(n1)

    val result = prod2.generate(0, 100)
    result should be (Generated(prod2, 0, 100, EventSet(100, 100, 0, prod2 -> 100.0)))

    val result2 = prod2.run(100, 100, 200)
    result2    should have size (1)

    val simEvent = result2(0).asInstanceOf[Produced]
    simEvent.es.size    should be (33.333 +- 0.001)
    simEvent.es.ts      should be (200.0)
    simEvent.es.latency should be (100.0)
    //result2(0).es.totals  should be (Map(prod2 -> 33.333))

    prod2.inputQueue should be (66.666 +- 0.001)
    prod2.outputQueues(n1) should be (33.333 +- 0.001)


  }

}
