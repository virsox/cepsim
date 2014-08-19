package ca.uwo.eng.sel.cepsim.query

import ca.uwo.eng.sel.cepsim.gen.Generator
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar


@RunWith(classOf[JUnitRunner])
class QueryTest extends FlatSpec
	with Matchers
	with MockitoSugar {
  
  trait Fixture {
    val generator = mock[Generator]
    doReturn(10).when(generator).generate()    
    val prod1 = EventProducer("p1", 10, generator)
    val cons1 = EventConsumer("c1", 10)
    val f1 = Operator("f1", 1000)

    var q = new Query
    q addVertices(prod1, f1, cons1)

  }
  
  
  "A query" should "correctly connect the vertices of the DAG" in new Fixture {
    q addEdge (prod1, f1)
    q addEdge (f1, cons1)
    
    q.vertices.size should be (3)
    q successors(prod1) should be (Set(f1))
    q successors(f1) should be (Set(cons1))
    q successors(cons1) should be(Set.empty)
  }
  
  
  it should "allow multiple output per operators" in new Fixture {

    val s1 = Operator("s1", 10000)
    val f2 = Operator("f2", 10000)
    val m1 = Operator("m1", 10000)

    q addVertices(prod1, f1, cons1)
    q addVertices(s1, f2, m1)
    q addEdges((prod1, s1, 1.0), (s1, f1, 1.0), (s1, f2, 1.0), (f1, m1, 1.0), (f2, m1, 1.0), (m1, cons1, 1.0))
    
    q.vertices.size should be (6)
    q successors(s1) should be (Set(f1, f2))
    q successors(f1) should be (Set(m1))
  }

  it should "return detailed connections information" in new Fixture {
    val s1 = Operator("s1", 10000)
    val f2 = Operator("f2", 10000)
    val m1 = Operator("m1", 10000)

    q addVertices(prod1, f1, cons1)
    q addVertices(s1, f2, m1)
    q addEdges((prod1, s1, 1.0), (s1, f1, 1.0), (s1, f2, 1.0), (f1, m1, 1.0), (f2, m1, 1.0), (m1, cons1, 1.0))

    q.edges(prod1) should have size (1)
    q.edges(prod1) should contain theSameElementsAs Set(Edge(prod1, s1, 1.0))

    q.edges(s1) should have size (2)
    q.edges(s1) should contain theSameElementsAs Set(Edge(s1, f1, 1.0), Edge(s1, f2, 1.0))
    //q.edges(s1) should be

    q.edges(f1) should have size (1)
    q.edges(f1) should contain theSameElementsAs Set(Edge(f1, m1, 1.0))

    q.edges(f2) should have size (1)
    q.edges(f2) should contain theSameElementsAs Set(Edge(f2, m1, 1.0))

    q.edges(m1) should have size (1)
    q.edges(m1) should contain theSameElementsAs Set(Edge(m1, cons1, 1.0))

  }



  
}