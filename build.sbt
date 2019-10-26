ThisBuild / scalaVersion := "2.13.1"
ThisBuild / version := "0.0.0"
ThisBuild / organization := "io.github.zero-deps"

Global / onChangedBuildSource := IgnoreSourceChanges

lazy val root = (project in file("."))
  .settings(
    name := "db"
  )
