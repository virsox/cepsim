package ca.uwo.eng.sel.cepsim.metric

import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by virso on 2014-08-01.
 */
class HistoryTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  "A History" should "log all events sent to it" in {
    val q = mock[Query]
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

    history.log("c2", 50.0, p1, 500)

    history.from(p1) should have size (2)
    history.from(p1) should be (List(History.Entry("c1", 0.0, p1, 500), History.Entry("c2", 50.0, p1, 500)))

    history.from(f1) should have size (1)
    history.from(f1) should be (List(History.Entry("c1", 10.0, f1, 100)))

    history.from(c1) should have size (1)
    history.from(c1) should be (List(History.Entry("c1", 30.0, c1, 100)))



  }


}
