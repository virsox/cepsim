package ca.uwo.eng.sel.cepsim.placement

import ca.uwo.eng.sel.cepsim.query.Query

trait OpPlacementStrategy {
  def execute(queries: Query*): Map[Query, List[Placement]] 
}