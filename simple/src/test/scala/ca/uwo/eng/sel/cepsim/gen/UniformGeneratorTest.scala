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
	  val generator = new UniformGenerator(20, 1 second)
	  
	  generator.generate() should be (20)
	  generator.generate() should be (20)
	  generator.generate() should be (20)
	}
	
	it should "calculate the correct value if interval is large" in {
	  val generator = new UniformGenerator(20, 5 seconds)
	  generator.generate() should be (100)
	}
	
	it should "calculate the correct value if interval is small" in {
    val generator = new UniformGenerator(20, 500 milliseconds)
    generator.generate() should be(10)
  }
	
	it should "calculate a correct sequence of values if interval is very small" in {
	  val generator = new UniformGenerator(20, 10 milliseconds)
	  generator.generate() should be (0)	  
	  generator.generate() should be (0)
	  generator.generate() should be (0)
	  generator.generate() should be (0)
	  generator.generate() should be (1)
	  generator.generate() should be (0)
	}

  it should "calculate the correct event generation average" in {
    val generator = new UniformGenerator(20, 1 second)
    for (i <- 1 to 10) generator.generate()
    generator.average should be (20.0)
  }

  it should "calculate the correct event generation average independently from the sampling rate" in {
    val generator = new UniformGenerator(20, 10 millisecond)
    for (i <- 1 to 10) generator.generate()
    generator.average should be (20.0)
  }

	
}