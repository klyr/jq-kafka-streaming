import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "org.barbot.jqstream",
      scalaVersion := "2.12.1",
      version      := "0.4.0"
    )),
    name := "KafkaStreamingJq",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.0.0",
    libraryDependencies += "net.thisptr" % "jackson-jq" % "0.0.8",
    libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.0.11",
    libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.11"
  )
