package ca.uwo.eng.sel.cepsim.event

import ca.uwo.eng.sel.cepsim.query.EventProducer
import ca.uwo.eng.sel.cepsim.util.SimEventBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2015-05-07.
 */
@RunWith(classOf[JUnitRunner])
class EventSetQueueTest extends FlatSpec
  with Matchers
  with MockitoSugar
  with SimEventBaseTest {

  trait Fixture {
    val prod1 = mock[EventProducer]("prod1")

    val queue = EventSetQueue()
    val es1 = EventSet(10.0, 100.0, 1.0, prod1 -> 10.0)
    val es2 = EventSet(10.0, 200.0, 2.0, prod1 -> 20.0)
  }

  "An EventSetQueues" should "enqueue event sets" in new Fixture {
    queue.enqueue(es1)
    queue.enqueue(es2)

    queue.size should be (20.0)
    queue.es   should equal (EventSet(20.0, 150.0, 1.5, prod1 -> 30.0))
  }


  it should "correctly dequeue the enqueued event sets" in new Fixture {
    queue.enqueue(es1)

    val result = queue.dequeue(10.0)
    result should be (es1)

    queue.size should be (0.0)
  }

  it should "combine event sets if necessary" in new Fixture {
    queue.enqueue(es1)
    queue.enqueue(es2)

    val result = queue.dequeue(20.0)
    result should be (EventSet(20.0, 150.0, 1.5, prod1 -> 30.0))

    queue.size should be (0.0)
  }


  it should "consume partial event sets" in new Fixture {
    queue.enqueue(es1)

    val result = queue.dequeue(5.0)
    result   should equal (EventSet(5.0, 100.0, 1.0, prod1 -> 5.0))

    queue.es should equal (EventSet(5.0, 100.0, 1.0, prod1 -> 5.0))
  }


  it should "dequeue enqueue events and consume partial event sets" in new Fixture {
    queue.enqueue(es1)
    queue.enqueue(es2)

    val result = queue.dequeue(15.0)
    result   should equal (EventSet(15.0, 133.3333, 1.3333, prod1 -> 20.0))

    queue.es should equal (EventSet( 5.0, 200.0, 2.0, prod1 -> 10.0))
  }


  it should "combine event sets and consume partial event sets" in new Fixture {
    val es3 = EventSet(10.0, 300.0, 1.0, prod1 -> 10.0)
    val es4 = EventSet(20.0, 400.0, 2.0, prod1 -> 20.0)

    queue.enqueue(es1)
    queue.enqueue(es2)
    queue.enqueue(es3)
    queue.enqueue(es4)

    val result = queue.dequeue(40.0)
    result   should equal (EventSet(40.0, 250.0, 1.5, prod1 -> 50.0))

    queue.es should equal (EventSet(10.0, 400.0, 2.0, prod1 -> 10.0))
  }

  it should "throw NoSuchElementExcetion if too many elements are dequeued" in new Fixture {
    queue.enqueue(es1)
    a [NoSuchElementException] should be thrownBy {
      queue.dequeue(20.0)
    }
  }

}
