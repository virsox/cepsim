package ca.uwo.eng.sel.cepsim.sched

import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query.Vertex

/**
  * Default allocation strategy. This strategy distributes the available instructions equally
  * among the queries of the placement. For each query, the allocated instructions are distributed
  * among the vertices proportionally according to the instructions/event metric. For example, if an
  * operator x requires 10 instructions/event, and an operator y requires 5, then x will receive twice
  * more instructions.
  */
class DefaultOpScheduleStrategy extends OpScheduleStrategy {

  override def allocate(instructions: Double, placement: Placement): List[(Vertex, Double)] =  {

    // allocate the same amount of instructions for each query
    val perQuery = instructions / placement.queries.size

    // calculate the amount of instructions per operator
    var instrPerOperator = Map.empty[Vertex, Double] withDefaultValue(0.0)
    placement.queries.foreach {(q) =>
      val total = placement.vertices(q).foldLeft(0.0){
        (sum, v) => sum + v.ipe
      }
      placement.vertices(q).foreach{(v) =>
        val vertexInstr = (v.ipe / total) * perQuery
        instrPerOperator = instrPerOperator updated (v, instrPerOperator(v) + vertexInstr)
      }
    }

    // build the return list
    placement.iterator.map((v) => (v, instrPerOperator(v))).toList
  }

}
