
import sbt._

organization := "de.deterministic-arts"

name := "scala-sql-library"

version := "0.3.1"

scalaVersion := "2.11.8"

scalaSource in Compile <<= baseDirectory(_ / "src")

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

