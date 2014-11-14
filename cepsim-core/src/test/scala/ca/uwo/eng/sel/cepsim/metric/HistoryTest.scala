package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.metric.History.{Received, Processed, Sent}
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.enablers.Sequencing
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.words.ArrayWrapper
import org.scalatest.{FlatSpec, Matchers}
import org.scalautils.Equality

import scala.collection.GenTraversable

/**
 * Created by virso on 2014-08-01.
 */
@RunWith(classOf[JUnitRunner])
class HistoryTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val p1 = mock[EventProducer]
    val f1 = mock[Operator]
    val c1 = mock[EventConsumer]
    doReturn("p1").when(p1).id
    doReturn("f1").when(f1).id
    doReturn("c1").when(c1).id    

    var history = History()
    history = history.logProcessed("c1", 0.0,  p1, 500)
    history = history.logProcessed("c1", 10.0, f1, 100)
    history = history.logProcessed("c1", 30.0, c1, 100)

  }
  
  "A History" should "log all events sent to it" in new Fixture {

    history = history.logSent("c2", 50.0, p1, f1, 500)
    history = history.logReceived("c2", 51.0, f1, p1, 500)
    history = history.logProcessed("c2", 55.0, f1, 500)

    history.from(p1) should have size (2)
    history.from(p1) should be (List(Processed("c1", 0.0, p1, 500), Sent("c2", 50.0, p1, f1, 500)))

    history.from(f1) should have size (3)
    history.from(f1) should be (List(Processed("c1", 10.0, f1, 100), Received("c2", 51.0, f1, p1, 500),
                                     Processed("c2", 55.0, f1, 500)))

    history.from(c1) should have size (1)
    history.from(c1) should be (List(Processed("c1", 30.0, c1, 100)))
  }


  it should "find processing entries only" in new Fixture {
    history = history.logSent("c2", 50.0, p1, f1, 500)
    history = history.logSent("c3", 60.0, p1, f1, 500)
    history = history.logReceived("c3", 62.0, p1, f1, 500)

    history.processedEntriesFrom(p1) should have size (1)
    history.processedEntriesFrom(p1) should be (List(Processed("c1", 0.0, p1, 500)))
  }

  it should "find the correct entry when using filters" in new Fixture {
    history = history.logProcessed("c1", 31.0, f1, 100)
    history = history.logProcessed("c1", 40.0, c1, 10)
    
    history.from(c1, 15.0) should be (Some(Processed("c1", 30.0, c1, 100)))
    history.from(c1, 40.0) should be (Some(Processed("c1", 40.0, c1, 10 )))
    history.from(c1, 50.0) should be (None)
  }
  
  it should "find the last entry" in new Fixture {
    history = history.logProcessed("c1", 31.0, c1, 100)
    history = history.logProcessed("c1", 35.0, f1, 100)
    
    history.lastFrom(c1) should be (Some(Processed("c1", 31.0, c1, 100)))
    history.lastFrom(p1) should be (Some(Processed("c1", 0.0,  p1, 500)))
  }
  
  it should "find the right successor" in new Fixture {    
  	val successor = history.successor(Processed("c1", 10.0, f1, 100))
	  successor should be (Some(Processed("c1", 30.0, c1, 100)))
  }  
  
  it should "find the right successor when there are many cloudlets in the history" in new Fixture {
    history = history.logProcessed("c1", 31.0, p1, 500)
    history = history.logProcessed("c2", 32.0, p1, 100)
    history = history.logProcessed("c1", 35.0, f1, 100)
    
	  val successor = history.successor(Processed("c1", 31.0, p1, 500))

	  successor should be (Some(Processed("c1", 35.0, f1, 100)))
    history.successor(Processed("c1", 35.0, f1, 100)) should be (None)
  }
    
  
  it should "merge with other histories" in new Fixture {
    
    val p2 = mock[EventProducer]
    val f2 = mock[Operator]
    val c2 = mock[EventConsumer]
    
    var history2 = History()
    history2 = history2.logSent("c2", 5.0,  p2, f2, 50)
    history2 = history2.logProcessed("c2", 10.0, f2, 10)
    history2 = history2.logProcessed("c2", 20.0, c2, 10)
    
    val result = history.merge(history2)
    
    // check history elements
    result should have size (6)
    result should be (List(
      Processed("c1", 0.0,  p1, 500),
      Sent     ("c2", 5.0,  p2, f2, 50),
      Processed("c1", 10.0, f1, 100),
      Processed("c2", 10.0, f2, 10),
      Processed("c2", 20.0, c2, 10),
      Processed("c1", 30.0, c1, 100)
    ))
    
    val opposite = history2.merge(history)   
    assert(result === opposite)
    
  }


  it should "correctly remove entries" in new Fixture {
    val e1 = Processed("c1", 0.0,  p1, 500)
    val e3 = Processed("c1", 30.0, c1, 100)

    history = history.remove(e1, e3)

    history should have size (1)
    history should be (List(Processed("c1", 10.0, f1, 100)))

  }
  
}
