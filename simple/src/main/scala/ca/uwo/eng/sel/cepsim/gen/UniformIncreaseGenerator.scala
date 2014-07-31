package ca.uwo.eng.sel.cepsim.gen

import scala.concurrent.duration.Duration

/**
 * Event generator in which the generation rates increase uniformly during a period, until it reaches
 * a maximum rate. From this period until the end of the simulation, the generation rate is kept at
 * the maximum.
 *
 * @param increaseDuration Period of time when the generation rate increases.
 * @param maxRate Maximum generation rate (in events per second).
 * @param samplingInterval  The simulation tick.
 */
class UniformIncreaseGenerator(val increaseDuration: Duration, val maxRate: Double,
    val samplingInterval: Duration) extends Generator {

  /** Alias. */
  type Point = (Double, Double)

  /** Keep track of the current simulation time */
  var currentPos = 0.0

  /** The maximum converted in events per milliseconds */
  val maxRateInMs = maxRate / 1000

  /** The increaseDuration converted to milliseconds */
  val durationInMs = increaseDuration.toMillis

  /** The simulation tick converted to milliseconds. */
  val intervalInMs = samplingInterval.toMillis

  /** Multiplier used during the rate growth period. */
  val multiplier = maxRateInMs / durationInMs

  /** Current calculated average */
  var currentAvg = 0.0

  /** Number of invocations*/
  var invocations = 0

  
  override def generate(): Int = {
    val nextPos = currentPos + intervalInMs
    var area = 0.0
    
    // Position still is at the increasing part of the graph
    if (currentPos < durationInMs) {

      // the sampling interval starts at the increasing part, and ends at the constant part
      if (nextPos > durationInMs) {
        area = (trapezoidArea((currentPos, 0), (currentPos, multiplier * currentPos),
                              (durationInMs, 0), (durationInMs, maxRateInMs)) +
                rectangleArea((durationInMs, 0), (durationInMs, maxRateInMs),
                              (nextPos, 0), (nextPos, maxRateInMs)))                
        
      } else {
        area = trapezoidArea((currentPos, 0), (currentPos, multiplier * currentPos),
                             (nextPos, 0), (nextPos, multiplier * nextPos))
      }

    // position is in the constant part
    } else {
      // integral is simply the are of the rectangle
      area = rectangleArea((currentPos, 0), (currentPos, maxRateInMs),
                    (nextPos, 0), (nextPos, maxRateInMs)) 
    }
    currentPos = nextPos

    val sampleAvg = area / samplingInterval.toSeconds
    currentAvg = ((invocations * currentAvg) + sampleAvg) / (invocations + 1)
    invocations = invocations + 1

    area toInt
  }

  override def average: Double = currentAvg

  /**
    * Calculates the area of a triangle rectangle. It assumes the following vertices parameters:
    *
    *           + v3
    *          /|
    *         / |
    *        /  |
    *    v1 +---+v2
    *
    * @param v1 First vertex.
    * @param v2 Second vertex.
    * @param v3 Third vertex.
    * @return The triangle area.
    */
  def triangleRectangleArea(v1: Point, v2: Point, v3: Point): Double =
    ((v2._1 - v1._1) * (v3._2 - v2._2 )) / 2


  /** *
    * Calculates the area of a rectangle. It assumes the following vertices parameters:
    *
    *    v2 +---+ v4
    *       |   |
    *       |   |
    *       |   |
    *    v1 +---+v3
    *
    * @param v1 First vertex.
    * @param v2 Second vertex.
    * @param v3 Third vertex.
    * @param v4 Fourth vertex.
    * @return The rectangle area.
    */
  def rectangleArea(v1: Point, v2: Point, v3: Point, v4: Point): Double =
    (v2._2 - v1._2) * (v3._1 - v1._1)

  /**
    * Calculates the area of a trapezoid. It assumes the following vertices parameters:
    *
    *           + v4
    *          /|
    *         / |
    *        /  |
    *    v2 +   + tempPoint
    *       |   |
    *    v1 +---+ v3
    *
    * @param v1 First vertex.
    * @param v2 Second vertex.
    * @param v3 Third vertex.
    * @param v4 Fourth vertex.
    * @return The area of the trapezoid.
    */
  def trapezoidArea(v1: Point, v2: Point, v3: Point, v4: Point): Double = {
    val tempPoint = (v3._1, v2._2)
    rectangleArea(v1, v2, v3, tempPoint) + triangleRectangleArea(v2, tempPoint, v4)
  }
  
}