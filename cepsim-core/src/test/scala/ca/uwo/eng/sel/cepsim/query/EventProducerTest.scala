package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.gen.Generator
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
    doReturn(100).when(generator).generate(anyInt())


    val prod = EventProducer("p1", 1, generator)
    val query = mock[Query]
    val n1 = mock[Operator]
    prod addOutputQueue (n1)
  }

  "An EventProducer" should "generate events and put in the input queue" in new Fixture {
    prod.generate()

    verify(generator).generate(anyInt())
    prod.inputQueue should be (100)
  }

  it should "process events and put them in the output queue" in new Fixture {
    prod.generate()
    prod.run(100)

    verify(generator).generate(anyInt())
    prod.inputQueue should be (0)
    prod.outputQueues(n1) should be (100)
  }

  it should "generate events and put half of them in the output queue" in new Fixture {
    prod.generate()
    prod.run(50)

    verify(generator).generate(anyInt())
    prod.inputQueue should be (50)
    prod.outputQueues(n1) should be (50)
  }

  it should "partially process input events" in new Fixture {
    val prod2 = EventProducer("p2", 3, generator)
    prod2.addOutputQueue(n1)
    prod2.generate()
    prod2.run(100)

    prod2.inputQueue should be (66.666 +- 0.001)
    prod2.outputQueues(n1) should be (33.333 +- 0.001)


  }

}
