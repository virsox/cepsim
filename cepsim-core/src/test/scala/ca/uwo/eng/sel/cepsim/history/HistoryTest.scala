package ca.uwo.eng.sel.cepsim.history

import ca.uwo.eng.sel.cepsim.metric.EventSet
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}


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
    val e1 = Generated(p1,  0.0, 10.0, EventSet(500.0, 10.0,  0.0, p1 -> 500.0))
    val e2 = Produced (p1, 10.0, 20.0, EventSet(100.0, 20.0, 10.0, p1 -> 100.0))
    val e3 = Produced (f1, 20.0, 30.0, EventSet( 50.0, 30.0, 20.0, p1 ->  50.0))
    val e4 = Produced (f1, 30.0, 40.0, EventSet( 50.0, 40.0, 30.0, p1 ->  50.0))
    val e5 = Consumed (c1, 40.0, 50.0, EventSet(100.0, 50.0, 40.0, p1 -> 100.0))

    history.log(e1)
    history.log(e2)
    history.log(e3)
    history.log(e4)
    history.log(e5)

  }


  "A History" should "log all events sent to it" in new Fixture {

    var result = history.from(p1).toList
    result should have size (2)
    result should contain theSameElementsInOrderAs (List(e1, e2))

    result = history.from(f1).toList
    result should have size (2)
    result should contain theSameElementsInOrderAs (List(e3, e4))

    result = history.from(c1).toList
    result should have size (1)
    result should contain theSameElementsInOrderAs (List(e5))
  }

  it should "find the correct entry when using filters" in new Fixture {

    history.from(f1, 20.0).toList should contain theSameElementsInOrderAs (List(e3, e4))
    history.from(c1, 30.0).toList should contain theSameElementsInOrderAs (List(e5))
    history.from(c1, 50.0).toList should have size (0)
  }

  it should "append other histories" in new Fixture {
    val history2 = History()

    val e6 = Produced (p1, 50.0, 60.0, EventSet(500, 60.0, 60.0, p1 -> 500.0))
    val e7 = Produced (f1, 60.0, 70.0, EventSet(500, 70.0, 70.0, p1 -> 500.0))
    val e8 = Consumed (c1, 70.0, 80.0, EventSet(500, 80.0, 80.0, p1 -> 500.0))

    history2.log(e6)
    history2.log(e7)
    history2.log(e8)

    history.append(history2)

    val result = history.toList
    result should have size (8)
    result should contain theSameElementsInOrderAs (List(e1, e2, e3, e4, e5, e6, e7, e8))
  }
  
  it should "merge with other histories" in new Fixture {
    
    val p2 = mock[EventProducer]
    val f2 = mock[Operator]
    val c2 = mock[EventConsumer]
    
    var history2 = History()

    val e6 = Generated(p2,  5.0, 15.0, EventSet(500.0, 15.0,  0.0, p2 -> 500.0))
    val e7 = Produced (p2, 25.0, 27.0, EventSet(500.0, 27.0, 12.0, p2 -> 500.0))
    val e8 = Produced (f2, 29.0, 35.0, EventSet(500.0, 35.0, 20.0, p2 -> 500.0))
    val e9 = Consumed (c2, 45.0, 50.0, EventSet(500.0, 50.0, 35.0, p2 -> 500.0))

    history2.log(e6)
    history2.log(e7)
    history2.log(e8)
    history2.log(e9)

    history.merge(history2)
    
    // check history elements
    val result = history.toList
    result should have size (9)
    result should contain theSameElementsInOrderAs (List(e1, e6, e2, e3, e7, e8, e4, e5, e9))

  }

  
}



