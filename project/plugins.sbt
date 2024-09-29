val zioSbtVersion = "0.4.0-alpha.28"

addSbtPlugin("dev.zio" % "zio-sbt-ecosystem" % zioSbtVersion)
addSbtPlugin("dev.zio" % "zio-sbt-website"   % zioSbtVersion)

addSbtPlugin("org.scoverage"    % "sbt-scoverage"    % "2.2.1")
addSbtPlugin("com.typesafe"     % "sbt-mima-plugin"  % "1.1.4")
addSbtPlugin("com.eed3si9n"     % "sbt-buildinfo"    % "0.12.0")
addSbtPlugin("org.scala-native" % "sbt-scala-native" % "0.5.5")
addSbtPlugin("org.scala-js"     % "sbt-scalajs"      % "1.17.0")

resolvers ++= Resolver.sonatypeOssRepos("public")
