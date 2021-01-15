scalaVersion := "3.0.0-M3"
version := zero.git.version()

libraryDependencies ++= Seq(
  "org.rocksdb" % "rocksdbjni" % "6.15.2"
, "dev.zio" %% "zio" % "1.0.3+130-a21e83b8-SNAPSHOT"
)
resolvers += Resolver.sonatypeRepo("snapshots")

lazy val ext = project.in(file("deps/ext"))
dependsOn(ext)

turbo := true
useCoursier := true
Global / onChangedBuildSource := IgnoreSourceChanges
