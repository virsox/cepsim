package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.Vm
import ca.uwo.eng.sel.cepsim.query.{EventConsumer, EventProducer, Operator, Query}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

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
        
    val query1 = mock[Query]
    doReturn(Set(prod1, f1, cons1)).when(query1).vertices
    doReturn(Set(query1)).when(prod1).queries
    doReturn(Set(query1)).when(f1).queries
    doReturn(Set(query1)).when(cons1).queries

    val query2 = mock[Query]
    doReturn(Set(prod2, f2, cons2)).when(query2).vertices
    doReturn(Set(query2)).when(prod2).queries
    doReturn(Set(query2)).when(f2).queries
    doReturn(Set(query2)).when(cons2).queries

    val placements = strategy.execute(query1, query2)
    placements should have size (1)

    val placement = placements.iterator.next

    placement.vertices should be (Set(prod1, prod2, f1, f2, cons1, cons2))
    placement.vertices(query1) should be (Set(prod1, f1, cons1))
    placement.vertices(query2) should be (Set(prod2, f2, cons2))
    placement.queries should be (Set(query1, query2))
    

    
  }
  
  
}