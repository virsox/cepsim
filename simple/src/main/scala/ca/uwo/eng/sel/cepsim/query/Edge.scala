package ca.uwo.eng.sel.cepsim.query

/**
 * Created by virso on 2014-07-23.
 */

object Edge {
  def apply(from: OutputVertex, to: InputVertex, selectivity: Double = 1.0) =
    new Edge(from, to, selectivity)

  implicit object EdgeVertexOrdering extends Ordering[Edge] {
    override def compare(x: Edge, y: Edge): Int = {
      val ret = x.from.compare(y.from)
      if (ret == 0) x.to.compare(y.to)
      else ret
    }
  }

}

class Edge(val from: OutputVertex, val to: InputVertex, val selectivity: Double = 1.0) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[Edge]

  override def equals(other: Any): Boolean = other match {
    case that: Edge =>
      (that canEqual this) &&
        from == that.from &&
        to == that.to &&
        selectivity == that.selectivity
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(from, to, selectivity)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"[from: $from, to: $to, selectivity: $selectivity]"
}
