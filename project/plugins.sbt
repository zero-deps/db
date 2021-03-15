resolvers += Resolver.githubPackages("zero-deps")
addSbtPlugin("com.codecommit" % "sbt-github-packages" % "latest.integration")
addSbtPlugin("io.github.zero-deps" % "sbt-git" % "latest.integration")

/* publishing */
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "latest.integration")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "latest.integration")
/* publishing */
