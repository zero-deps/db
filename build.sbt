scalaVersion := "3.0.0-RC1"
version := zero.git.version()

libraryDependencies ++= Seq(
  "org.rocksdb" % "rocksdbjni" % "6.15.5"
, "dev.zio" %% "zio-streams"  % "1.0.5"
, "dev.zio" %% "zio-test-sbt" % "1.0.5" % Test
)
testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

lazy val ext = project.in(file("deps/ext"))
lazy val macros = project.in(file("deps/proto/macros"))

dependsOn(ext, macros)

scalacOptions ++= Seq(
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

resolvers += Resolver.JCenterRepository
