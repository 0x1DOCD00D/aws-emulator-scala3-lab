// Additional subprojects for the CS441 Docker Hadoop extension.
// This file is loaded by sbt alongside the existing build.sbt.
// Do not duplicate these definitions in build.sbt.

lazy val cs441HadoopJavaOptions = Seq(
  Compile / run / fork := true,
  Compile / run / javaOptions ++= sys.env
    .get("AWS_EMULATOR_JAVA_TRUSTSTORE")
    .toSeq
    .flatMap(path => Seq(
      s"-Djavax.net.ssl.trustStore=$path",
      "-Djavax.net.ssl.trustStoreType=PKCS12"
    ))
)

lazy val hadoopCluster = (project in file("hadoop-cluster"))
  .settings(cs441HadoopJavaOptions)
  .settings(
    name := "cs441-hadoop-cluster",
    scalaVersion := "3.9.0",
    Compile / run / mainClass := Some("edu.cs441.hadoop.cluster.HadoopClusterCli"),
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "sts" % "2.55.12",
      "software.amazon.awssdk" % "s3"  % "2.55.12"
    )
  )

// Hadoop dependencies are provided by the Docker image at job execution time.
// sbt-assembly includes the Scala 3 runtime in the MapReduce JAR.
lazy val hadoopJob = (project in file("hadoop-cluster/jobs/wordcount"))
  .settings(
    name := "cs441-hadoop-wordcount",
    scalaVersion := "3.9.0",
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-common" % "3.4.2" % Provided,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.2" % Provided
    ),
    assembly / mainClass := None,
    assembly / assemblyJarName := "cs441-wordcount.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) if xs.nonEmpty &&
        (xs.last.toUpperCase.endsWith(".SF") ||
         xs.last.toUpperCase.endsWith(".RSA") ||
         xs.last.toUpperCase.endsWith(".DSA")) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case x => (assembly / assemblyMergeStrategy).value(x)
    }
  )
