ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "edu.cs441"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "aws-emulator-scala3-lab",
    Compile / run / fork := true,
    Compile / run / javaOptions ++= sys.env
      .get("AWS_EMULATOR_JAVA_TRUSTSTORE")
      .toSeq
      .flatMap(path => Seq(
        s"-Djavax.net.ssl.trustStore=$path",
        "-Djavax.net.ssl.trustStoreType=PKCS12"
      )),
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "sts"      % "2.55.12",
      "software.amazon.awssdk" % "s3"       % "2.55.12",
      "software.amazon.awssdk" % "dynamodb" % "2.55.12",
      "software.amazon.awssdk" % "sqs"      % "2.55.12",
      "org.slf4j" % "slf4j-simple" % "1.7.36" % Runtime
    )
  )
