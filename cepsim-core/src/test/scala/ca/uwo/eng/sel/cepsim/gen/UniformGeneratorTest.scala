package ca.uwo.eng.sel.cepsim.gen

import scala.concurrent.duration._
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UniformGeneratorTest extends FlatSpec
	with Matchers {
  
	"A UniformGenerator" should "always generate the same value" in {
	  val generator = UniformGenerator(20)
	  
	  generator.generate(1000) should be (20.00 +- 0.001)
	  generator.generate(1000) should be (20.00 +- 0.001)
	  generator.generate(1000) should be (20.00 +- 0.001)
	}
	
	it should "calculate the correct value if interval is large" in {
	  val generator = UniformGenerator(20)
	  generator.generate(5000) should be (100.00 +- 0.001)
	}
	
	it should "calculate the correct value if interval is small" in {
    val generator = UniformGenerator(20)
    generator.generate(500) should be(10.00 +- 0.001)
  }
	
	it should "generate partial events if interval is very small" in {
	  val generator = UniformGenerator(20)
	  generator.generate(10) should be (0.2 +- 0.001)
	  generator.generate(10) should be (0.2 +- 0.001)
	}

  it should "calculate the correct event generation average" in {
    val generator = UniformGenerator(20)
    for (i <- 1 to 10) generator.generate(1000)
    generator.average should be (20.0)
  }

  it should "calculate the correct event generation average independently from the sampling rate" in {
    val generator = UniformGenerator(20)
    for (i <- 1 to 10) generator.generate(10)
    generator.average should be (20.0)
  }

	
}