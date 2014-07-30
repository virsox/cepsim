package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.gen.Generator
import org.junit.runner.RunWith
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
    doReturn(100).when(generator).generate()

    val prod = new EventProducer("p1", 1, generator)
    val query = mock[Query]
    val n1 = mock[Operator]
    prod addOutputQueue (n1)


    prod.init(query)
  }

  "An EventProducer" should "generate events and put in the output queue" in new Fixture {
    prod.run(1000)

    verify(generator).generate()
    prod.inputQueue should be (0)
    prod.outputQueues(n1) should be (100)
  }

  it should "generate events and put half of them in the output queue" in new Fixture {
    prod.run(50)

    verify(generator).generate()
    prod.inputQueue should be (50)
    prod.outputQueues(n1) should be (50)
  }


}
