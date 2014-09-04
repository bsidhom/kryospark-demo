name := "kryospark-demo"

organization := "io.sidhom"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.4"

runMain in Compile <<= Defaults.runMainTask(
  fullClasspath in Compile, runner in (Compile, run))
