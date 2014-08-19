package ca.uwo.eng.sel.cepsim

class Host(val vms: Set[Vm]) {
  def getVmMips(vmId: Long): Double = vms.find(_.id == vmId) match {
    case Some(x) => x.mips
    case None => throw new IllegalArgumentException("VM not found")
  }
}