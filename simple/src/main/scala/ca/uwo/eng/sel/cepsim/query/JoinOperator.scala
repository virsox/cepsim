package ca.uwo.eng.sel.cepsim.query

/**
 * Created by virso on 2014-07-22.
 */
class JoinOperator(override val id: String, override val ipe: Double) extends Operator(id, ipe) {

  // todo implement a join operator
  // a case class can't inherit for another - what should I do?
  override def init(q: Query) = { }
  override def run(instructions: Double): Unit = { }

}
