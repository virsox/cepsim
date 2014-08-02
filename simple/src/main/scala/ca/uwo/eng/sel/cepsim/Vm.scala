package ca.uwo.eng.sel.cepsim

/**
 * Created by virso on 2014-08-01.
 */

object Vm {
  def apply(id: String, mips: Double) = new Vm(id, mips)
}

class Vm(val id: String, val mips: Double) {

}
