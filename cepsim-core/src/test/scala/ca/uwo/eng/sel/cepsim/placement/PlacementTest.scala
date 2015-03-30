package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.Vm
import ca.uwo.eng.sel.cepsim.query._
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by virso on 2014-07-24.
 */
@RunWith(classOf[JUnitRunner])
class PlacementTest extends FlatSpec
  with Matchers
  with MockitoSugar {

  trait Fixture {
    val q1 = mock[Query]("q1")
    val q2 = mock[Query]("q2")
    val prod1 = mock[EventProducer]("prod1")
    val prod2 = mock[EventProducer]("prod2")
    val s1 = mock[Operator]("s1")
    val f1 = mock[Operator]("f1")
    val f2 = mock[Operator]("f2")
    val m1 = mock[Operator]("m1")
    val cons1 = mock[EventConsumer]("cons1")
    val cons2 = mock[EventConsumer]("cons2")
    //val vm = mock[Vm]

    doReturn(100L).when(q1).duration
    doReturn(200L).when(q2).duration
    doReturn("prod1").when(prod1).id
    doReturn("prod2").when(prod2).id
    doReturn("s1").when(s1).id
    doReturn("f1").when(f1).id
    doReturn("f2").when(f1).id
    doReturn("m1").when(m1).id
    doReturn("cons1").when(cons1).id
    doReturn("cons2").when(cons2).id
  }

  "A Placement" should "manage all added vertices" in new Fixture {
    var p = Placement(Set.empty[Vertex], 1)

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
    p.producers    should be (Set(prod1, prod2))
    p.queries      should be (Set(q1, q2))
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

    val placement = Placement(q1, 1)
    val it = placement.iterator

    it.next should be (prod1)
    it.next should be (f1)
    it.next should be (cons1)
    it.hasNext should be (false)

  }

  it should "return an iterator that doesn't include vertices that aren't in the placement" in new Fixture {
    doReturn(Set(q1)).when(prod1).queries
    doReturn(Set(q1)).when(f1).queries
    doReturn(Set(q1)).when(f2).queries
    doReturn(Set(q1)).when(cons1).queries
    doReturn(Set(q1)).when(cons2).queries

    doReturn(Set(prod1, f1, f2, cons1, cons2)).when(q1).vertices

    doReturn(Set.empty).when(q1).predecessors(prod1)
    doReturn(Set(prod1)).when(q1).predecessors(f1)
    doReturn(Set(f1)).when(q1).predecessors(cons1)
    doReturn(Set(f1)).when(q1).predecessors(f2)
    doReturn(Set(f2)).when(q1).predecessors(cons2)

    doReturn(Set(f1)).when(q1).successors(prod1)
    doReturn(Set(f2, cons1)).when(q1).successors(f1)
    doReturn(Set(cons2)).when(q1).successors(f2)
    doReturn(Set.empty).when(q1).successors(cons1)
    doReturn(Set.empty).when(q1).successors(cons2)


    val placement = Placement(Set(prod1, f1, cons1), 1)
    val it = placement.iterator

    it.next should be (prod1)
    it.next should be (f1)
    it.next should be (cons1)
    it.hasNext should be (false)

  }

  it should "return an iterator that iterates in topological order when queries share operators" in new Fixture {

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

    val placement = Placement(Set(prod1, prod2, s1, f1, f2, m1, cons1, cons2), 1)
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


  it should "return an iterator that iterates in topological order on a irregular graph" in new Fixture {
    val f3 = mock[Operator]("f3")

    doReturn(Set(q1)).when(prod1).queries
    doReturn(Set(q1)).when(prod2).queries
    doReturn(Set(q1)).when(f1).queries
    doReturn(Set(q1)).when(f2).queries
    doReturn(Set(q1)).when(f3).queries
    doReturn(Set(q1)).when(m1).queries
    doReturn(Set(q1)).when(cons1).queries

    doReturn(Set(prod1, prod2, f1, f2, f3, m1, cons1)).when(q1).vertices

    doReturn(Set.empty).when(q1).predecessors(prod1)
    doReturn(Set.empty).when(q1).predecessors(prod2)
    doReturn(Set(prod1)).when(q1).predecessors(f1)
    doReturn(Set(prod2)).when(q1).predecessors(f2)
    doReturn(Set(f2)).when(q1).predecessors(f3)
    doReturn(Set(f1, f3)).when(q1).predecessors(m1)
    doReturn(Set(m1)).when(q1).predecessors(cons1)

    doReturn(Set(f1)).when(q1).successors(prod1)
    doReturn(Set(f2)).when(q1).successors(prod2)
    doReturn(Set(f3)).when(q1).successors(f2)
    doReturn(Set(m1)).when(q1).successors(f1)
    doReturn(Set(m1)).when(q1).successors(f3)
    doReturn(Set(cons1)).when(q1).successors(m1)
    doReturn(Set.empty).when(q1).successors(cons1)

    val placement = Placement(Set(prod1, prod2, f1, f2, f3, m1, cons1), 1)
    val it = placement.iterator

    Set(it.next, it.next) should contain allOf (prod1, prod2)
    Set(it.next, it.next) should contain allOf (f1, f2)
    it.next should be (f3)
    it.next should be (m1)
    it.next should be (cons1)
    it.hasNext should be (false)

  }

  it should "follow an user-defined iteration order" in new Fixture {
    doReturn(Set(q1)).when(prod1).queries
    doReturn(Set(q1)).when(f1).queries
    doReturn(Set(q1)).when(f2).queries
    doReturn(Set(q1)).when(cons1).queries
    val placement = Placement(Set(prod1, f1, f2, cons1), 1, List(cons1, f1, f2, prod1))

    val it = placement.iterator
    it.next should be (cons1)
    it.next should be (f1)
    it.next should be (f2)
    it.next should be (prod1)
    it.hasNext should be (false)
  }

  it should "calculate the correct duration" in new Fixture {
    var p = Placement(Set.empty[Vertex], 1)

    // adding only the producers
    doReturn(Set(q1)).when(prod1).queries
    doReturn(Set(q2)).when(prod2).queries

    p = p addVertex prod1
    p = p addVertex prod2

    p.duration should be (200L)
  }

}
