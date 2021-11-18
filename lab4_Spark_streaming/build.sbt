ThisBuild / organization := "pl.edu.agh"
ThisBuild / scalaVersion := "2.12.10"
ThisBuild / version := "0.1.0-SNAPSHOT"

val circeVersion = "0.12.3"

lazy val root = (project in file("."))
  .settings(
    name := "coinbase-spark",
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-sqs" % "1.11.897",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.scalatest" %% "scalatest" % "3.2.2" % "test",
      "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
      "org.apache.spark" %% "spark-streaming" % "3.2.0" % "provided",
      "com.squareup.okhttp3" % "okhttp" % "4.6.0",
    )
  )

assemblyMergeStrategy in assembly := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case x                      => MergeStrategy.first
}

resolvers += Resolver.sonatypeRepo("releases")
addCompilerPlugin(
  "org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full
)
