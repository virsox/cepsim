package ca.uwo.eng.sel.cepsim

import scala.concurrent.duration._

import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.EventConsumer
import ca.uwo.eng.sel.cepsim.query.EventProducer
import ca.uwo.eng.sel.cepsim.query.Operator


@RunWith(classOf[JUnitRunner])
class QueryCloudletTest extends FlatSpec
	with Matchers
	with MockitoSugar {
  
  "A QueryCloudlet" should "run operators according to the specified strategy" in {
    var cloudlet = new QueryCloudlet(500 milliseconds)
    
    val prod = mock[EventProducer]
    val f1 = mock[Operator]
    val f2 = mock[Operator]
    val cons = mock[EventConsumer]    
    val placement = mock[Placement]
    doReturn(Seq(prod, f1, f2, cons)).when(placement).vertices()
    
        
    cloudlet setAvailableMips(1000)
    cloudlet setPlacement(placement)
    
    verify(placement, atLeastOnce()).vertices()
    verify(prod).run(100)
    verify(f1).run(400)
    verify(f2).run(400)
    verify(prod).run(100)
    
    
  }
  
}