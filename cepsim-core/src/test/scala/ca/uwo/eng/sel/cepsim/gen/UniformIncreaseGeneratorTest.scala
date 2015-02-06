package ca.uwo.eng.sel.cepsim.gen

import scala.concurrent.duration._
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UniformIncreaseGeneratorTest extends FlatSpec
	with Matchers {

  "A UniformIncreaseGenerator" should "have two distinct regions" in {
    val generator = UniformIncreaseGenerator(10 seconds, 20, 5 seconds)
    
    // first region is the triangle with vertices (0, 0), (10, 0), (10, 20)    
    // --- first sample is the triangle (0, 0), (5, 0), (5, 10)
    generator.generate() should be (25.0)
    
    // -- second sample is the trapezoid (5, 0), (5, 10), (10, 0), (10, 20)
    generator.generate() should be (75.0)
 
    
    // second region of the function is constant    
    // -- third sample is the rectangle (10, 0), (10, 20), (15, 0), (15, 20)
    generator.generate() should be (100.0)
    
    // -- all the other samples should be 100
    generator.generate() should be (100.0)
  }
  
  
  it should "give the correct values even when a sample encompasses both regions" in {
    val generator = UniformIncreaseGenerator(10 seconds, 20, 6 seconds)
    
    generator.generate() should be ( 36.0)
    generator.generate() should be (104.0)
    generator.generate() should be (120.0)
  }

  it should "calculate the correct average if only the increasing region is used" in {
    val generator = UniformIncreaseGenerator(10 seconds, 20, 5 seconds)
    generator.generate()
    generator.generate()
    generator.average should be (10.0)
  }

  it should "calculate the correct average if both regions are used" in {
    val generator = UniformIncreaseGenerator(10 seconds, 20, 5 seconds)

    generator.generate()
    generator.generate()
    generator.generate()
    generator.generate()
    generator.average should be (15.0 +- 0.001)
  }

  it should "have an average close to the maxRate if is used for a long time" in {
    val generator = UniformIncreaseGenerator(10 seconds, 20, 5 seconds)
    for (i <- 1 to 100000) generator.generate()

    generator.average should be (20.0 +- 0.1)
  }

}