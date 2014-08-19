package ca.uwo.eng.sel.cepsim.perf

import ca.uwo.eng.sel.cepsim.query.Operator
import java.util.Date
import scala.collection.mutable.Queue

object FilterPerformance extends App {

  class Event {
    var values: Map[String, Any] = Map.empty    
    values += ("s1" -> "ABCDEF")
    values += ("s2" -> 15)
    values += ("s3" -> new Date())
    
    def get(key: String) = values(key)    
  }
  
  class Filter extends Operator("f", 100, 0) {
    val d = new Date()
    
    def apply(e: Event) =
      e.get("s1").equals("ABCDEF") &&
      e.get("s2") == 15 &&
      e.get("s3").asInstanceOf[Date].getDay() == 15
  }
  
  
  override def main(args: Array[String]) {
    val f = new Filter()
    //val e = new Event()
    
    val a = System.nanoTime
    var q = Queue[Event]()
    
    for (i <- 1 to 1000000) {
      val e = new Event()
      
      q.enqueue(e)
      f.apply(e)
      q.dequeue()
    }
    
    val b = System.nanoTime
    println("time: "+ (b - a) / 1e6 + "ms")

    // 1 microsecond per event = 1e-6 seconds

    // 1 second = 2.3 GHz ~ 2,300,000,000 insructions / sec

    // 1    - 2.3e9
    // 1e-6 - x

    // x = 2.3e3 = 2300 instructions
    
  }
}
  
  
