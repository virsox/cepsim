package ca.uwo.eng.sel.cepsim

import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import ca.uwo.eng.sel.cepsim.gen.Generator
import ca.uwo.eng.sel.cepsim.query.Query
import ca.uwo.eng.sel.cepsim.query.Operator
import ca.uwo.eng.sel.cepsim.query.EventProducer
import ca.uwo.eng.sel.cepsim.query.EventConsumer


@RunWith(classOf[JUnitRunner])
class QueryTest extends FlatSpec
	with Matchers
	with MockitoSugar {
  
  trait Fixture {
    val generator = mock[Generator]
    doReturn(10).when(generator).generate()    
    val prod1 = new EventProducer(generator)    
    val cons1 = new EventConsumer()
    val f1 = new Operator("f1")

    var q = new Query
    q = q addVertices(prod1, f1, cons1)

  }
  
  
  "A query" should "correctly connect the vertices of the DAG" in new Fixture {
    q = q addEdge (prod1, f1)
    q = q addEdge (f1, cons1)
    
    q.vertices.size should be (3)
    q connections(prod1) should be (Set(f1))
    q connections(f1) should be (Set(cons1))
    q connections(cons1) should be(Set.empty)    
  }
  
  
  it should "allow multiple output per operators" in new Fixture {

    val s1 = new Operator("s1")
    val f2 = new Operator("f2")
    val m1 = new Operator("m1")

    q = q addVertices(prod1, f1, cons1)
    q = q addVertices(s1, f2, m1)
    q = q addEdges((prod1, s1), (s1, f1), (s1, f2), (f1, m1), (f2, m1), (m1, cons1))
    
    q.vertices.size should be (6)
    q connections(s1) should be (Set(f1, f2))
    q connections(f1) should be (Set(m1))
  }


  it should "return an iterator that iterates through the vertices" in new Fixture {

    q = q addEdges((prod1, f1), (f1, cons1))

    val it = q.iterator()

    it.next should be (prod1)
    it.next should be (f1)
    it.next should be (cons1)
    it.hasNext should be (false)
  }

  
  it should "return an iterator that iterates in BFS" in new Fixture {
    val generator2 = mock[Generator]
    val prod2 = new EventProducer(generator2)
    val s1 = new Operator("s1")
    val f2 = new Operator("f2")
    val m1 = new Operator("m1")

    q = q addVertices(prod2, s1, f2, m1)
    q = q addEdges((prod1, s1), (prod2, s1), (s1, f1), (s1, f2), (f1, m1), (f2, m1), (m1, cons1))


    val it = q.iterator()

    Set(it.next, it.next) should contain allOf (prod1, prod2)
    it.next should be (s1)
    Set(it.next, it.next) should contain allOf (f1, f2)
    it.next should be (m1)
    it.next should be (cons1)
    it.hasNext should be (false)
  }
  
  
}