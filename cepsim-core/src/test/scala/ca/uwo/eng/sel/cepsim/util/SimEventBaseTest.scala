package ca.uwo.eng.sel.cepsim.util

import ca.uwo.eng.sel.cepsim.history.{Consumed, Produced}
import org.scalautils.Equality

/**
 * Created by virso on 15-04-04.
 */
trait SimEventBaseTest {


  implicit def producedEquality = new Equality[Produced] {
    override def areEqual(a: Produced, b: Any): Boolean = b match {
      case b: Produced => a.v    == b.v    &&
        a.from == b.from &&
        a.to   == b.to   &&
        Math.abs(a.quantity - b.quantity) < 0.001 &&
        a.processed.forall((e) => Math.abs(e._2 - b.processed.getOrElse(e._1, 0.0)) < 0.001)
      case _ => false
    }
  }

  implicit def consumedEquality = new Equality[Consumed] {
    override def areEqual(a: Consumed, b: Any): Boolean = b match {
      case b: Consumed => a.v    == b.v    &&
        a.from == b.from &&
        a.to   == b.to   &&
        Math.abs(a.quantity - b.quantity) < 0.001 &&
        a.processed.forall((e) => Math.abs(e._2 - b.processed.getOrElse(e._1, 0.0)) < 0.001)
      case _ => false
    }
  }
}
