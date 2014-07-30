package ca.uwo.eng.sel.cepsim

class Host(val mips: Double) {
  def getVmMips(vmId: Long): Double = mips
}