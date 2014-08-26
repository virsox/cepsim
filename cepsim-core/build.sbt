name := "cep-simulator-simple"

version := "1.0.0"

scalaVersion := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-deprecation", "-feature")

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.5" % Test

libraryDependencies += "junit" % "junit" % "4.11" % Test

libraryDependencies += "org.mockito" % "mockito-core" % "1.9.5" % Test