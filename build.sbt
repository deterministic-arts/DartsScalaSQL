
import java.io.File
import sbt._
import Process._
import Keys._

organization := "de.deterministic-arts"  

name := "DartsLibSQL"

version := "0.2"

scalaVersion := "2.11.1"

scalaSource in Compile <<= baseDirectory (_ / "src")
