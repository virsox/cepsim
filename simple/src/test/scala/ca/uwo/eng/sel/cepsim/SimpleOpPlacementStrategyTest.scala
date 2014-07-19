package ca.uwo.eng.sel.cepsim

import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import ca.uwo.eng.sel.cepsim.placement.SimpleOpPlacementStrategy
import ca.uwo.eng.sel.cepsim.query.Query
import ca.uwo.eng.sel.cepsim.query.EventProducer
import ca.uwo.eng.sel.cepsim.query.Operator
import ca.uwo.eng.sel.cepsim.query.EventConsumer

@RunWith(classOf[JUnitRunner])
class SimpleOpPlacementStrategyTest extends FlatSpec
	with Matchers
	with MockitoSugar {

  "A SimpleOpPlacementStrategy" should "place all operators in the same VM" in {
    val strategy = new SimpleOpPlacementStrategy(1L)
    val prod1, prod2 = mock[EventProducer]
    val f1, f2 = mock[Operator]
    val cons1, cons2 = mock[EventConsumer]
        
    val query1 = mock[Query]
    doReturn(Set()).when(query1).vertices
    
    val query2 = mock[Query]
    doReturn(Set()).when(query2).vertices
    
    val placements = strategy.execute(query1, query2)
    val placement1 = placements(query1)
    val placement2 = placements(query2)
    
    placements.size should be (2)
    placement1.size should be (1)
    placement2.size should be (1)
    
    
    placement1(0).vertices should be (List(prod1, f1, cons1))
    placement1(0).vmId should be (1L)
    
    placement2(1).vertices should be (Seq(prod2, f2, cons2))
    placement2(1).vmId should be (1L)
    
    
    
  }
  
  
}