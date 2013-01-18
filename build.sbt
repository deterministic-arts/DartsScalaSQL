
import java.io.File
import sbt._
import Process._
import Keys._

libraryDependencies += "joda-time"  % "joda-time"     % "2.1"

libraryDependencies += "org.joda"   % "joda-convert"  % "1.2"

organization := "de.deterministic-arts"  

name := "DartsLibSQL"

version := "0.2"

scalaVersion := "2.9.2"

scalaSource in Compile <<= baseDirectory (_ / "src")
