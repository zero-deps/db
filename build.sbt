scalaVersion := "3.0.0-M3"
version := zero.git.version()

libraryDependencies ++= Seq(
  "org.rocksdb" % "rocksdbjni" % "6.14.6"
, "dev.zio" %% "zio-streams"  % "1.0.4-2"
, "dev.zio" %% "zio-test-sbt" % "1.0.4-2" % Test
)
testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

lazy val ext = project.in(file("deps/ext"))
dependsOn(ext)

lazy val proto3 = project.in(file("deps/proto/scala3macros"))
dependsOn(proto3)

scalacOptions ++= Seq(
  "-language:postfixOps"
, "-Yexplicit-nulls"
, "-language:strictEquality"
)

turbo := true
useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges
