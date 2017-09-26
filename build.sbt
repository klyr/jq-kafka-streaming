import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "org.barbot.jqstream",
      scalaVersion := "2.12.1",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "KafkaStreamingJq",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.11.0.0",
    libraryDependencies += "net.thisptr" % "jackson-jq" % "0.0.7",
    libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.0.10",
    libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.10"
  )
