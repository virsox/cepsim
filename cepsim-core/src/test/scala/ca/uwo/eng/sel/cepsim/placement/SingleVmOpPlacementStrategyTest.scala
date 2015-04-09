package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class SingleVmOpPlacementStrategyTest extends FlatSpec
	with Matchers
	with MockitoSugar {

  "A SimpleOpPlacementStrategy" should "place all operators in the same VM" in {
    //val vm = mock[Vm]
    val strategy = new SingleVmOpPlacementStrategy(1)
    val prod1, prod2 = mock[EventProducer]
    val f1, f2 = mock[Operator]
    val cons1, cons2 = mock[EventConsumer]


    doReturn("prod1").when(prod1).id
    doReturn("prod2").when(prod2).id
    doReturn("f1").when(f1).id
    doReturn("f2").when(f2).id
    doReturn("cons1").when(cons1).id
    doReturn("cons2").when(cons2).id

    val query1 = mock[Query]
    doReturn(Set(prod1, f1, cons1)).when(query1).vertices
    doReturn(Set(query1)).when(prod1).queries
    doReturn(Set(query1)).when(f1).queries
    doReturn(Set(query1)).when(cons1).queries

    doReturn(Set.empty).when(query1).predecessors(prod1)
    doReturn(Set(prod1)).when(query1).predecessors(f1)
    doReturn(Set(f1)).when(query1).predecessors(cons1)

    doReturn(Set(f1)).when(query1).successors(prod1)
    doReturn(Set(cons1)).when(query1).successors(f1)
    doReturn(Set.empty).when(query1).successors(cons1)


    val query2 = mock[Query]
    doReturn(Set(prod2, f2, cons2)).when(query2).vertices
    doReturn(Set(query2)).when(prod2).queries
    doReturn(Set(query2)).when(f2).queries
    doReturn(Set(query2)).when(cons2).queries

    doReturn(Set.empty).when(query2).predecessors(prod2)
    doReturn(Set(prod2)).when(query2).predecessors(f2)
    doReturn(Set(f2)).when(query2).predecessors(cons2)

    doReturn(Set(f2)).when(query2).successors(prod2)
    doReturn(Set(cons2)).when(query2).successors(f2)
    doReturn(Set.empty).when(query2).successors(cons2)



    val placements = strategy.execute(query1, query2)
    placements should have size (1)

    val placement = placements.iterator.next

    placement.vertices should be (Set(prod1, prod2, f1, f2, cons1, cons2))
    placement.vertices(query1) should be (Set(prod1, f1, cons1))
    placement.vertices(query2) should be (Set(prod2, f2, cons2))
    placement.queries should be (Set(query1, query2))
    

    
  }
  
  
}