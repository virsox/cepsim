package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.query.Query

/* Interface for operator placement strategies. */
trait OpPlacementStrategy {

  /**
    * Define placement for all vertices from the queries.
    * @param queries Set of queries from which the placement should be executed.
    * @return List of placements (mappings of vertices to virtual machines)
    */
  def execute(queries: Query*): Set[Placement]
}