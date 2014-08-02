package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.Vm
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, Query, Operator, EventProducer}
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
    val q = mock[Query]
    val prod1 = mock[EventProducer]
    val prod2 = mock[EventProducer]
    val s1 = mock[Operator]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val m1 = mock[Operator]
    val cons1 = mock[EventConsumer]
    val vm = mock[Vm]

    doReturn("prod1").when(prod1).id
    doReturn("prod2").when(prod2).id
    doReturn("s1").when(s1).id
    doReturn("f1").when(f1).id
    doReturn("f2").when(f1).id
    doReturn("m1").when(m1).id
    doReturn("cons1").when(cons1).id
  }


  "A Placement" should "return an iterator that iterates through the vertices" in new Fixture {

    doReturn(Set(prod1, f1, cons1)).when(q).vertices

    doReturn(Set.empty).when(q).predecessors(prod1)
    doReturn(Set(prod1)).when(q).predecessors(f1)
    doReturn(Set(f1)).when(q).predecessors(cons1)

    doReturn(Set(f1)).when(q).successors(prod1)
    doReturn(Set(cons1)).when(q).successors(f1)
    doReturn(Set.empty).when(q).successors(cons1)

    val placement = Placement(q, vm)
    val it = placement.iterator

    it.next should be (prod1)
    it.next should be (f1)
    it.next should be (cons1)
    it.hasNext should be (false)

  }



  it should "return an iterator that iterates in BFS" in new Fixture {
    doReturn(Set(prod1, prod2, s1, f1, f2, m1, cons1)).when(q).vertices

    doReturn(Set.empty).when(q).predecessors(prod1)
    doReturn(Set.empty).when(q).predecessors(prod2)
    doReturn(Set(prod1, prod2)).when(q).predecessors(s1)
    doReturn(Set(s1)).when(q).predecessors(f1)
    doReturn(Set(s1)).when(q).predecessors(f2)
    doReturn(Set(f1, f2)).when(q).predecessors(m1)
    doReturn(Set(m1)).when(q).predecessors(cons1)

    doReturn(Set(s1)).when(q).successors(prod1)
    doReturn(Set(s1)).when(q).successors(prod2)
    doReturn(Set(f1, f2)).when(q).successors(s1)
    doReturn(Set(m1)).when(q).successors(f1)
    doReturn(Set(m1)).when(q).successors(f2)
    doReturn(Set(cons1)).when(q).successors(m1)
    doReturn(Set.empty).when(q).successors(cons1)

    val placement = Placement(q, vm)
    val it = placement.iterator

    it.next should be (prod1)
    it.next should be (prod2)
    it.next should be (s1)
    Set(it.next, it.next) should contain allOf (f1, f2)
    it.next should be (m1)
    it.next should be (cons1)
    it.hasNext should be (false)
  }

}
