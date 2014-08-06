package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.Vm
import ca.uwo.eng.sel.cepsim.query._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2014-07-24.
 */
class PlacementTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val q1 = mock[Query]
    val q2 = mock[Query]
    val prod1 = mock[EventProducer]
    val prod2 = mock[EventProducer]
    val s1 = mock[Operator]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val m1 = mock[Operator]
    val cons1 = mock[EventConsumer]
    val cons2 = mock[EventConsumer]
    val vm = mock[Vm]

    doReturn("prod1").when(prod1).id
    doReturn("prod2").when(prod2).id
    doReturn("s1").when(s1).id
    doReturn("f1").when(f1).id
    doReturn("f2").when(f1).id
    doReturn("m1").when(m1).id
    doReturn("cons1").when(cons1).id
  }

  "A Placement" should "manage all added vertices" in new Fixture {
    var p = Placement(Set.empty[Vertex], vm)

    doReturn(Set(q1)).when(prod1).queries
    doReturn(Set(q1, q2)).when(f1).queries
    doReturn(Set(q1)).when(cons1).queries

    doReturn(Set(q2)).when(prod2).queries
    doReturn(Set(q2)).when(cons2).queries

    p = p addVertex prod1
    p = p addVertex f1
    p = p addVertex cons1

    p = p addVertex prod2
    p = p addVertex f1
    p = p addVertex cons2

    p.vertices     should be (Set(prod1, f1, cons1, prod2, cons2))
    p.vertices(q1) should be (Set(prod1, f1, cons1))
    p.vertices(q2) should be (Set(prod2, f1, cons2))
    p.queries should be (Set(q1, q2))
  }



  it should "return an iterator that iterates through the vertices" in new Fixture {

    doReturn(Set(q1)).when(prod1).queries
    doReturn(Set(q1)).when(f1).queries
    doReturn(Set(q1)).when(cons1).queries

    doReturn(Set(prod1, f1, cons1)).when(q1).vertices

    doReturn(Set.empty).when(q1).predecessors(prod1)
    doReturn(Set(prod1)).when(q1).predecessors(f1)
    doReturn(Set(f1)).when(q1).predecessors(cons1)

    doReturn(Set(f1)).when(q1).successors(prod1)
    doReturn(Set(cons1)).when(q1).successors(f1)
    doReturn(Set.empty).when(q1).successors(cons1)

    val placement = Placement(q1, vm)
    val it = placement.iterator

    it.next should be (prod1)
    it.next should be (f1)
    it.next should be (cons1)
    it.hasNext should be (false)

  }



  it should "return an iterator that iterates in BFS" in new Fixture {

    doReturn(Set(q1)).when(prod1).queries
    doReturn(Set(q1)).when(cons1).queries
    doReturn(Set(q2)).when(prod2).queries
    doReturn(Set(q2)).when(cons2).queries
    doReturn(Set(q1, q2)).when(s1).queries
    doReturn(Set(q1, q2)).when(f1).queries
    doReturn(Set(q1, q2)).when(f2).queries
    doReturn(Set(q1, q2)).when(m1).queries

    doReturn(Set(prod1, s1, f1, f2, m1, cons1)).when(q1).vertices
    doReturn(Set(prod2, s1, f1, f2, m1, cons2)).when(q2).vertices

    doReturn(Set.empty).when(q1).predecessors(prod1)
    doReturn(Set(prod1)).when(q1).predecessors(s1)
    doReturn(Set(s1)).when(q1).predecessors(f1)
    doReturn(Set(s1)).when(q1).predecessors(f2)
    doReturn(Set(f1, f2)).when(q1).predecessors(m1)
    doReturn(Set(m1)).when(q1).predecessors(cons1)

    doReturn(Set.empty).when(q2).predecessors(prod2)
    doReturn(Set(prod1)).when(q2).predecessors(s1)
    doReturn(Set(s1)).when(q2).predecessors(f1)
    doReturn(Set(s1)).when(q2).predecessors(f2)
    doReturn(Set(f1, f2)).when(q2).predecessors(m1)
    doReturn(Set(m1)).when(q2).predecessors(cons2)

    doReturn(Set(s1)).when(q1).successors(prod1)
    doReturn(Set(f1, f2)).when(q1).successors(s1)
    doReturn(Set(m1)).when(q1).successors(f1)
    doReturn(Set(m1)).when(q1).successors(f2)
    doReturn(Set(cons1)).when(q1).successors(m1)
    doReturn(Set.empty).when(q1).successors(cons1)

    doReturn(Set(s1)).when(q2).successors(prod2)
    doReturn(Set(f1, f2)).when(q2).successors(s1)
    doReturn(Set(m1)).when(q2).successors(f1)
    doReturn(Set(m1)).when(q2).successors(f2)
    doReturn(Set(cons2)).when(q2).successors(m1)
    doReturn(Set.empty).when(q2).successors(cons2)

    val placement = Placement(Set(prod1, prod2, s1, f1, f2, m1, cons1, cons2), vm)
    val it = placement.iterator

    it.next should be (prod1)
    it.next should be (prod2)
    it.next should be (s1)
    Set(it.next, it.next) should contain allOf (f1, f2)
    it.next should be (m1)
    it.next should be (cons1)
    it.next should be (cons2)
    it.hasNext should be (false)
  }

}
