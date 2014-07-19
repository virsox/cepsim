package ca.uwo.eng.sel.cepsim.gen

import scala.concurrent.duration.Duration

class UniformIncreaseGenerator(val increaseDuration: Duration, val maxRate: Double,
    val samplingInterval: Duration) {

  type Point = (Double, Double)
  
  var currentPos = 0.0
  
  val maxRateInMs = maxRate / 1000
  val durationInMs = increaseDuration.toMillis
  val intervalInMs = samplingInterval.toMillis
  val multi = maxRateInMs / durationInMs
  
  
  def generate(): Int = {
    val nextPos = currentPos + intervalInMs
    var area = 0.0
    
    // Position still is at the increasing part of the graph
    if (currentPos < durationInMs) {
      if (nextPos > durationInMs) {
        area = (trapezoidArea((currentPos, 0), (currentPos, multi * currentPos),
                              (durationInMs, 0), (durationInMs, maxRateInMs)) +
                rectangleArea((durationInMs, 0), (durationInMs, maxRateInMs),
                              (nextPos, 0), (nextPos, maxRateInMs)))                
        
      } else {
        area = trapezoidArea((currentPos, 0), (currentPos, multi * currentPos),
                             (nextPos, 0), (nextPos, multi * nextPos))        
      }
      
    } else {
      area = rectangleArea((currentPos, 0), (currentPos, maxRateInMs),
                    (nextPos, 0), (nextPos, maxRateInMs)) 
    }
    currentPos = nextPos
    area toInt
  }
  
  def triangleRectangleArea(v1: Point, v2: Point, v3: Point): Double =
    ((v2._1 - v1._1) * (v3._2 - v2._2 )) / 2 
    
  def rectangleArea(v1: Point, v2: Point, v3: Point, v4: Point): Double =
    (v2._2 - v1._2) * (v3._1 - v1._1)
    
  def trapezoidArea(v1: Point, v2: Point, v3: Point, v4: Point): Double = {
    val tempPoint = (v3._1, v2._2)
    rectangleArea(v1, v2, v3, tempPoint) + triangleRectangleArea(v2, tempPoint, v4)
  }
  
}