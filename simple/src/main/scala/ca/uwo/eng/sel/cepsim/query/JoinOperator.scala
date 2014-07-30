package ca.uwo.eng.sel.cepsim.query

object JoinOperator {
  def apply(id: String, ipe: Double, reduction: Double) =
    new JoinOperator(id, ipe, reduction)

}

class JoinOperator(override val id: String, override val ipe: Double, val reduction: Double)
    extends Operator(id, ipe)
    with InputVertex
    with OutputVertex {

  override def run(instructions: Double): Unit = {
    val events = retrieveFromInput(instructions)

    // calculate the cartesian product among all input events
    val total = events.foldLeft(1)((accum, elem) => accum * elem._2)

    // the reduction parameter represents how much the join condition reduces
    // the number of joined elements
    sendToAllOutputs(Math.floor(total * reduction).toInt)
  }

}
