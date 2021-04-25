lazy val root = project.in(file(".")).settings(
  scalaVersion := "3.0.0-RC2"
).aggregate(db)

lazy val db = project.in(file("db")).settings(
  scalaVersion := "3.0.0-RC2"
, scalacOptions ++= opts
, libraryDependencies ++= Seq(
    "org.rocksdb" % "rocksdbjni" % "6.19.3"
  , "dev.zio" %% "zio-streams"  % "1.0.6"
  , "dev.zio" %% "zio-test-sbt" % "1.0.6" % Test
  )
, testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
).dependsOn(proto)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  libraryDependencies += "com.google.protobuf" % "protobuf-java" % "3.15.6"
, scalaVersion := "3.0.0-RC2"
).dependsOn(protoops)

lazy val protoops = project.in(file("deps/proto/ops")).settings(
  scalaVersion := "3.0.0-RC2"
).dependsOn(protosyntax)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := "3.0.0-RC2"
)

val opts = Seq(
  "-language:postfixOps"
, "-language:strictEquality"
, "-Yexplicit-nulls"
, "-source", "future-migration"
, "-deprecation"
, "-rewrite"
, "release", "11"
)

turbo := true
useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges
