package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.gen.Generator
import ca.uwo.eng.sel.cepsim.history.{Produced, Generated}
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

    val query = mock[Query]
    val n1 = mock[Operator]


    def setup(ipe: Double, limit: Boolean, generate: Double): EventProducer = {
      val prod = EventProducer("p1", ipe, generator, limit)
      prod addOutputQueue (n1)
      doReturn(generate).when(generator).generate(anyDouble(), anyInt())

      prod
    }

  }

  "An EventProducer" should "generate events and put in the input queue" in new Fixture {
    val prod = setup(1, false, 100.0)
    val result = prod.generate(0, 1000)
    result.get should be (Generated(prod, 0, 1000, EventSet(100, 1000, 0, prod -> 100.0)))

    verify(generator).generate(org.mockito.Matchers.eq(1000.0), anyInt())
    prod.inputQueue should be (100)
  }

  it should "respect buffer limits from successors when generating events" in new Fixture {

    val prod = setup(1, true, 100)
    prod.inputEventSet.add(EventSet(50, 5.0, 0.0, prod -> 50.0))
    prod.setLimit(n1, 100)

    prod.generate(1000, 2000)
    verify(generator).generate(org.mockito.Matchers.eq(1000.0), org.mockito.Matchers.eq(50.0))

    prod.inputEventSet.reset()
    prod.inputEventSet.add(EventSet(120, 5.0, 0.0, prod -> 50.0))
    prod.generate(1000, 2000)
    verify(generator).generate(org.mockito.Matchers.eq(1000.0), org.mockito.Matchers.eq(0.0))
  }

  it should "accumulate until at least one event is generated" in new Fixture {
    val prod = setup(1, false, 0.25)

    var simEvent = prod.generate(1000, 2000)
    simEvent should be(None)

    simEvent = prod.generate(2000, 3000)
    simEvent should be(None)

    simEvent = prod.generate(3000, 4000)
    simEvent should be(None)

    simEvent = prod.generate(4000, 5000)
    simEvent should be(Some(Generated(prod, 4000.0, 5000.0, EventSet(1.0, 5000.0, 0.0, prod -> 1.0))))

    verify(generator, times(4)).generate(org.mockito.Matchers.eq(1000.0), anyInt())
  }

  it should "accumulate correctly through more than one iteration" in new Fixture {
    val prod = setup(1, false, 0.4)

    var simEvent = prod.generate(1000, 2000)
    simEvent should be (None)

    simEvent = prod.generate(2000, 3000)
    simEvent should be (None)

    simEvent = prod.generate(3000, 4000)
    simEvent.get should be (Generated(prod, 3000.0, 4000.0, EventSet(1.0, 4000.0, 0.0, prod -> 1.0)))

    simEvent = prod.generate(4000, 5000)
    simEvent should be (None)

    simEvent = prod.generate(5000, 6000)
    simEvent.get should be (Generated(prod, 5000.0, 6000.0, EventSet(1.0, 6000.0, 0.0, prod -> 1.0)))

    verify(generator, times(5)).generate(org.mockito.Matchers.eq(1000.0), anyInt())
  }



  it should "process events and put them in the output queue" in new Fixture {
    val prod = setup(1, false, 100.0)
    val result1 = prod.generate(0, 1000)
    result1.get should be (Generated(prod, 0, 1000, EventSet(100, 1000, 0, prod -> 100.0)))

    val result2 = prod.run(100, 1000, 1100)
    result2    should have size (1)
    result2(0) should be (new Produced(prod, 1000, 1100, EventSet(100, 1100, 100, prod -> 100.0)))

    verify(generator).generate(org.mockito.Matchers.eq(1000.0), anyInt())
    prod.inputQueue should be (0)
    prod.outputQueues(n1) should be (100)
  }

  it should "generate events and put half of them in the output queue" in new Fixture {
    val prod = setup(1, false, 100.0)
    val result1 = prod.generate(0, 500)
    result1.get should be (Generated(prod, 0, 500, EventSet(100, 500, 0, prod -> 100.0)))

    val result2 = prod.run(50, 500, 550)
    result2    should have size (1)
    result2(0) should be (new Produced(prod, 500, 550, EventSet(50, 550, 50, prod -> 50.0)))

    verify(generator).generate(org.mockito.Matchers.eq(500.0), anyInt())
    prod.inputQueue should be (50)
    prod.outputQueues(n1) should be (50)
  }

  it should "partially process input events" in new Fixture {
    val prod = setup(3, false, 100.0)
    prod.addOutputQueue(n1)

    val result = prod.generate(0, 100)
    result.get should be (Generated(prod, 0, 100, EventSet(100, 100, 0, prod -> 100.0)))

    val result2 = prod.run(100, 100, 200)
    result2    should have size (1)

    val simEvent = result2(0).asInstanceOf[Produced]
    simEvent.es.size    should be (33.333 +- 0.001)
    simEvent.es.ts      should be (200.0)
    simEvent.es.latency should be (100.0)
    //result2(0).es.totals  should be (Map(prod2 -> 33.333))

    prod.inputQueue should be (66.666 +- 0.001)
    prod.outputQueues(n1) should be (33.333 +- 0.001)
  }





}
