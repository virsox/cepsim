package ca.uwo.eng.sel.cepsim.placement

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
    val strategy = new SingleVmOpPlacementStrategy(1L)
    val prod1, prod2 = mock[EventProducer]
    val f1, f2 = mock[Operator]
    val cons1, cons2 = mock[EventConsumer]
        
    val query1 = mock[Query]
    doReturn(Set(prod1, f1, cons1)).when(query1).vertices
    
    val query2 = mock[Query]
    doReturn(Set(prod2, f2, cons2)).when(query2).vertices
    
    val placements = strategy.execute(query1, query2)
    val placement1 = placements(query1)
    val placement2 = placements(query2)
    
    placements should have size (2)
    placement1 should have size (1)
    placement2 should have size (1)
    
    
    placement1(0).vertices should be (Set(prod1, f1, cons1))
    placement1(0).vmId should be (1L)
    
    placement2(0).vertices should be (Set(prod2, f2, cons2))
    placement2(0).vmId should be (1L)
    
    
    
  }
  
  
}