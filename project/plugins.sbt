resolvers += "JBoss" at "https://repository.jboss.org/"

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "1.6.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.4.1")
