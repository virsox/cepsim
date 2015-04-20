package ca.uwo.eng.sel.cepsim.util

import ca.uwo.eng.sel.cepsim.history.{Consumed, Produced}
import ca.uwo.eng.sel.cepsim.metric.EventSet
import org.scalautils.Equality

/**
 * Created by virso on 15-04-04.
 */
trait SimEventBaseTest {


  def compare(a: Double, b: Double): Boolean = Math.abs(a - b) < 0.001

  def compare(a: EventSet, b: EventSet): Boolean = compare(a.size, b.size) &&
    a.ts == b.ts &&
    compare(a.latency, b.latency) &&
    a.totals.forall((e) => compare(e._2, b.totals.getOrElse(e._1, -1.0)))


  implicit def eventSetEquality = new Equality[EventSet] {
    override def areEqual(a: EventSet, b: Any): Boolean = b match {
      case b: EventSet => compare(a, b)
      case _ => false
    }
  }


  implicit def producedEquality = new Equality[Produced] {
    override def areEqual(a: Produced, b: Any): Boolean = b match {
      case b: Produced => a.v == b.v    &&
        a.from == b.from &&
        a.to   == b.to   &&
        compare(a.es, b.es)
      case _ => false
    }
  }

  implicit def consumedEquality = new Equality[Consumed] {
    override def areEqual(a: Consumed, b: Any): Boolean = b match {
      case b: Consumed => a.v == b.v    &&
        a.from == b.from &&
        a.to   == b.to   &&
        compare(a.quantity, b.quantity) &&
        compare(a.es, b.es)
      case _ => false
    }
  }
}
