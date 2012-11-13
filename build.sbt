
import java.io.File
import sbt._
import Process._
import Keys._

libraryDependencies += "joda-time" % "joda-time" % "1.6" 

name := "DartsLibSQL"

version := "0.1"

scalaVersion := "2.9.2"

scalaSource in Compile <<= baseDirectory (_ / "src")

scalaSource in Test <<= baseDirectory (_ / "test")

