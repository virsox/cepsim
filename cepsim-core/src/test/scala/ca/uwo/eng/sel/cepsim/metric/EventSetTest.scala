package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.query.EventProducer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2015-04-17.
 */
@RunWith(classOf[JUnitRunner])
class EventSetTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val prod1 = mock[EventProducer]("prod1")
    val prod2 = mock[EventProducer]("prod2")
  }

  "A EventSet" should "correctly execute the add method" in new Fixture {

    val es1 = EventSet( 5.0,  5.0, 0.0, prod1 -> 5.0)
    val es2 = EventSet(10.0, 20.0, 0.0, prod1 -> 10.0)

    es1.add(es2)

    es1.size    should be (15)
    es1.ts      should be (15.0)
    es1.latency should be (0.0)
    es1.totals  should be (Map(prod1 -> 15.0))

    // es2 is unchanged
    es2 should be (EventSet(10.0, 20.0, 0.0, prod1 -> 10.0))
  }

  it should "also consider latency in the add method" in new Fixture {

    val es1 = EventSet(10.0, 15.0, 5.0, prod1 -> 10.0)
    val es2 = EventSet(10.0, 20.0, 8.0, prod1 -> 10.0)

    es1.add(es2)
    es1 should be (EventSet(20.0, 17.5, 6.5, prod1 -> 20.0))
  }

  it should "correctly update the set timestamp" in new Fixture {
    val es1 = EventSet(20.0, 17.5, 6.5, prod1 -> 20.0)
    es1.updateTimestamp(25.0)
    es1 should be (EventSet(20.0, 25.0, 14.0, prod1 -> 20.0))
  }

  it should "correctly extract a number of events" in new Fixture {
    val es1 = EventSet(20, 10.0, 5.0, prod1 -> 40.0, prod2 -> 10.0)

    val p1 = es1.extract(5)

    es1 should be (EventSet(15.0, 10.0, 5.0, prod1 -> 30.0, prod2 -> 7.5))
    p1  should be (EventSet( 5.0, 10.0, 5.0, prod1 -> 10.0, prod2 -> 2.5))
  }

  it should "split the event set in two according to the informed percentage" in new Fixture {
    val es1 = EventSet(10.0, 15.0, 5.0, prod1 -> 10.0)

    val (p1, p2) = es1.split(0.5)
    p1 should be (EventSet(5.0, 15.0, 5.0, prod1 -> 5.0))
    p2 should be (EventSet(5.0, 15.0, 5.0, prod1 -> 5.0))

    val (p3, p4) = es1.split(0.2)
    p3 should be (EventSet(2.0, 15.0, 5.0, prod1 -> 2.0))
    p4 should be (EventSet(8.0, 15.0, 5.0, prod1 -> 8.0))
  }


  it should "split the event set in two when percentage is 100%" in new Fixture {
    val es1 = EventSet(10.0, 15.0, 5.0, prod1 -> 10.0)

    val (p1, p2) = es1.split(1.0)
    p1 should be (EventSet(10.0, 15.0, 5.0, prod1 -> 10.0))
    p2 should be (EventSet( 0.0, 15.0, 5.0, prod1 ->  0.0))
  }

}
