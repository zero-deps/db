resolvers += Resolver.githubPackages("zero-deps")
addSbtPlugin("com.codecommit" % "sbt-github-packages" % "latest.integration")
addSbtPlugin("io.github.zero-deps" % "sbt-git" % "latest.integration")
addSbtPlugin("ch.epfl.lamp" % "sbt-dotty" % "latest.integration")
