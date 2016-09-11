
import sbt._

organization := "de.deterministic-arts"

name := "scala-sql-library"

version := "0.2"

scalaVersion := "2.11.8"

scalaSource in Compile <<= baseDirectory(_ / "src")
