scalaVersion := "3.0.0-RC1"
version := zero.git.version()

libraryDependencies ++= Seq(
  "org.rocksdb" % "rocksdbjni" % "6.14.6"
, "dev.zio" %% "zio-streams"  % "1.0.5"
, "dev.zio" %% "zio-test-sbt" % "1.0.5" % Test
)
testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

lazy val ext = project.in(file("deps/ext"))
dependsOn(ext)

lazy val macros = project.in(file("deps/proto/macros"))
dependsOn(macros)

scalacOptions ++= Seq(
  "-language:postfixOps"
, "-Yexplicit-nulls"
, "-language:strictEquality"
)

turbo := true
useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges

resolvers += Resolver.JCenterRepository
