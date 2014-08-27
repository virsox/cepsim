package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

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

    val history = History()
    history.log("c1", 0.0,  p1, 500)
    history.log("c1", 10.0, f1, 100)
    history.log("c1", 30.0, c1, 100)

  }
  
  "A History" should "log all events sent to it" in new Fixture {


    history.log("c2", 50.0, p1, 500)

    history.from(p1) should have size (2)
    history.from(p1) should be (List(History.Entry("c1", 0.0, p1, 500), History.Entry("c2", 50.0, p1, 500)))

    history.from(f1) should have size (1)
    history.from(f1) should be (List(History.Entry("c1", 10.0, f1, 100)))

    history.from(c1) should have size (1)
    history.from(c1) should be (List(History.Entry("c1", 30.0, c1, 100)))
  }

  it should "find the correct entry when using filters" in new Fixture {
    history.log("c1", 31.0, f1, 100)
    history.log("c1", 40.0, c1, 10)
    
    history.from(c1, 15.0) should be (Some(History.Entry("c1", 30.0, c1, 100)))   
    history.from(c1, 40.0) should be (Some(History.Entry("c1", 40.0, c1, 10 )))
    history.from(c1, 50.0) should be (None)
  }
  
  it should "find the right successor" in new Fixture {    
	val successor = history.successor(History.Entry("c1", 10.0, f1, 100))	  
	successor should be (Some(History.Entry("c1", 30.0, c1, 100)))    
  }
  
  
  it should "find the right successor when there are many cloudlets in the history" in new Fixture {
    history.log("c1", 31.0, p1, 500)
    history.log("c2", 32.0, p1, 100)
    history.log("c1", 35.0, f1, 100)
    
	val successor = history.successor(History.Entry("c1", 31.0, p1, 500))	  
	successor should be (Some(History.Entry("c1", 35.0, f1, 100)))        
    
    history.successor(History.Entry("c1", 35.0, f1, 100)) should be (None)
  }
    
  
  it should "merge with other histories" in new Fixture {
    
    val p2 = mock[EventProducer]
    val f2 = mock[Operator]
    val c2 = mock[EventConsumer]
    
    val history2 = History()
    history2.log("c2", 5.0,  p2, 50)
    history2.log("c2", 10.0, f2, 10)
    history2.log("c2", 20.0, c2, 10)
    
    val result = history.merge(history2)
    
    // check history elements
    result.entries should have size (6)
    result.entries should contain theSameElementsInOrderAs List(
      History.Entry("c1", 0.0,  p1, 500),
      History.Entry("c2", 5.0,  p2, 50),
      History.Entry("c1", 10.0, f1, 100),
      History.Entry("c2", 10.0, f2, 10),
      History.Entry("c2", 20.0, c2, 10),
      History.Entry("c1", 30.0, c1, 100)
    )
    
    val opposite = history2.merge(history)   
    assert(result.entries === opposite.entries)
    
  }
  
  
}
