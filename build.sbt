lazy val `db-root` = project.in(file(".")).aggregate(db)

lazy val db = project.in(file("db")).settings(
  scalaVersion := scala
, scalacOptions ++= opts
, libraryDependencies ++= Seq(
    "org.rocksdb" % "rocksdbjni" % "9.6.1"
  , "dev.zio" %% "zio-streams"  % "2.1.9"
  , "dev.zio" %% "zio-test-sbt" % "2.1.9" % Test
  )
, testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
).dependsOn(proto)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  libraryDependencies += "com.google.protobuf" % "protobuf-java" % "4.28.2"
, scalaVersion := scala
, crossScalaVersions := scala :: Nil
).dependsOn(`proto-syntax`)

lazy val `proto-syntax` = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := scala
, crossScalaVersions := scala :: Nil
)

val scala = "3.3.3"

val opts = Seq(
  "-language:strictEquality"
, "-Wunused:imports"
, "-Xfatal-warnings"
, "-Yexplicit-nulls"
, "release", "17"
)

ThisBuild / turbo := true
Global / onChangedBuildSource := ReloadOnSourceChanges
