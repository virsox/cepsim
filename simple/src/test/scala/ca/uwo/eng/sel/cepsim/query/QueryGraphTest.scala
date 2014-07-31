package ca.uwo.eng.sel.cepsim.query

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2014-07-30.
 */
class QueryGraphTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  "A query" should "enumerate all paths from the consumer to producers" in {
    var q = new Query()

    val p1 = mock[EventProducer]
    val p2 = mock[EventProducer]
    val m1 = mock[Operator]
    val s1 = mock[Operator]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val m2 = mock[Operator]
    val c1 = mock[EventConsumer]
    doReturn("p1").when(p1).id
    doReturn("p2").when(p2).id
    doReturn("m1").when(m1).id
    doReturn("s1").when(s1).id
    doReturn("f1").when(f1).id
    doReturn("f2").when(f2).id
    doReturn("m2").when(m2).id
    doReturn("c1").when(c1).id
    q = q.addVertices(p1, p2, m1, s1, f1, f2, m2, c1)

    val e1 = Edge(p1, m1, 1.0)
    val e2 = Edge(p2, m1, 1.0)
    val e3 = Edge(m1, s1, 1.0)
    val e4 = Edge(s1, f1, 0.5)
    val e5 = Edge(s1, f2, 0.5)
    val e6 = Edge(f1, m2, 0.5)
    val e7 = Edge(f2, m2, 0.6)
    val e8 = Edge(m2, c1, 1.0)
    q = q.addEdges(Set(e1, e2, e3, e4, e5, e6, e7, e8))

    val paths: List[VertexPath] = q.pathsToProducers(c1)

    paths should have size (4)
    paths(0).vertices should contain theSameElementsInOrderAs (List(c1, m2, f1, s1, m1, p1))
    paths(0).edges    should contain theSameElementsInOrderAs (List(e8, e6, e4, e3, e1))

    paths(1).vertices should contain theSameElementsInOrderAs (List(c1, m2, f1, s1, m1, p2))
    paths(1).edges    should contain theSameElementsInOrderAs (List(e8, e6, e4, e3, e2))

    paths(2).vertices should contain theSameElementsInOrderAs (List(c1, m2, f2, s1, m1, p1))
    paths(2).edges    should contain theSameElementsInOrderAs (List(e8, e7, e5, e3, e1))

    paths(3).vertices should contain theSameElementsInOrderAs (List(c1, m2, f2, s1, m1, p2))
    paths(3).edges    should contain theSameElementsInOrderAs (List(e8, e7, e5, e3, e2))
  }


}
