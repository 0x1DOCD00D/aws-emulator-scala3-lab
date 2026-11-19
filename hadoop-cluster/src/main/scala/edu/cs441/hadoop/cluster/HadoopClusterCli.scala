package edu.cs441.hadoop.cluster

import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Main program of the separate hadoopCluster sbt subproject. */
object HadoopClusterCli:
  private val usage = """
    |CS441 local Hadoop / AWS Emulator demonstration
    |Run from the repository root with:
    |  sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli COMMAND ...'
    |
    |  setup                                  Clone pinned Apache Docker branch, build image
    |  up [workers]                           Start HDFS/YARN (default 2, maximum 4)
    |  status                                 Inspect Docker, HDFS and YARN state
    |  down                                   Stop/remove Hadoop containers (data may be lost)
    |  smoke                                  Execute and assert Apache's built-in WordCount
    |  wordcount <local-input>                Run compiled Scala 3 WordCount JAR
    |  stage <local-input> <hdfs-file-path>    Copy host file into HDFS
    |  run-jar <jar> <class> <hdfs-in> <out>   Run an existing Hadoop JAR on real YARN
    |  show <hdfs-output-directory>           Print Hadoop part files
    |  s3-put <local> <bucket> <key>          Upload test input to emulated S3
    |  s3-wordcount <bucket> <key> <prefix>   S3 -> HDFS -> Scala job -> S3
    |  self-test                              Pure Scala checks, no Docker or AWS required
    |
    |Build Scala 3 JAR before running wordcount or s3-wordcount:
    |  sbt 'hadoopJob/assembly'
    |
    |This program NEVER runs arbitrary Hadoop JARs inside AWS Emulator's EMR API model.
    |""".stripMargin

  def main(args: Array[String]): Unit =
    try execute(args.toList)
    catch
      case NonFatal(e) =>
        System.err.println(s"[ERROR] ${e.getMessage}")
        System.err.println("Run with 'help' to see arguments and expected setup.")
        System.exit(1)

  private def wordCountJar(hadoop: HadoopDocker): Path =
    hadoop.moduleRoot.resolve(
      "jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar").toAbsolutePath

  /** Accept paths relative to either the repository root or the Hadoop module. */
  private def resolveLocalFile(hadoop: HadoopDocker, supplied: String): Path =
    val raw = Paths.get(supplied)
    val candidates =
      if raw.isAbsolute then Seq(raw)
      else Seq(
        Paths.get("").toAbsolutePath.resolve(raw),
        hadoop.moduleRoot.resolve(raw),
        hadoop.moduleRoot.getParent.resolve(raw)
      )
    candidates.iterator.map(_.toAbsolutePath.normalize())
      .find(path => Files.isRegularFile(path))
      .getOrElse(throw new IllegalArgumentException(
        s"Cannot find local file '$supplied'. Tried: ${candidates.mkString(", ")}"))

  private def execute(args: List[String]): Unit =
    if args.isEmpty || args.head == "help" || args.head == "--help" then
      println(usage)
      return
    if args == List("self-test") then
      checkPureFunctions()
      return

    val hadoop = HadoopDocker.locate()
    args match
      case List("setup") => hadoop.setup()
      case List("up") => hadoop.up()
      case List("up", workers) => hadoop.up(workers.toInt)
      case List("status") => hadoop.status()
      case List("down") => hadoop.down()
      case List("smoke") =>
        val input = hadoop.newInputPath()
        val output = hadoop.newOutputPath()
        hadoop.putLocalFile(hadoop.moduleRoot.resolve("data/wordcount.txt"), input)
        hadoop.runBundledWordCount(input, output)
        val text = hadoop.resultText(output)
        print(text)
        hadoop.assertSampleCounts(text)
        println(s"[smoke] PASS; HDFS output=$output")

      case List("wordcount", inputFile) =>
        val file = resolveLocalFile(hadoop, inputFile)
        val jar = wordCountJar(hadoop)
        require(Files.isRegularFile(jar), s"Build it first with sbt 'hadoopJob/assembly': $jar")
        val input = hadoop.newInputPath()
        val output = hadoop.newOutputPath()
        hadoop.putLocalFile(file, input)
        hadoop.runScalaWordCount(jar, input, output)
        val text = hadoop.resultText(output)
        print(text)
        if file.getFileName.toString == "wordcount.txt" then hadoop.assertSampleCounts(text)
        println(s"[wordcount] PASS; HDFS output=$output")

      case List("stage", localFile, hdfsFile) =>
        hadoop.putLocalFile(resolveLocalFile(hadoop, localFile), hdfsFile)

      case List("run-jar", jar, mainClass, hdfsInput, hdfsOutput) =>
        hadoop.runJar(resolveLocalFile(hadoop, jar), mainClass, hdfsInput, hdfsOutput)
        println(hadoop.resultText(hdfsOutput))

      case List("show", hdfsOutput) => print(hadoop.resultText(hdfsOutput))

      case List("s3-put", localFile, bucket, key) =>
        val s3 = EmulatorS3.open()
        try s3.uploadFile(resolveLocalFile(hadoop, localFile), bucket, key)
        finally s3.close()

      case List("s3-wordcount", bucket, key, outputPrefix) =>
        val jar = wordCountJar(hadoop)
        require(Files.isRegularFile(jar), s"Build it first with sbt 'hadoopJob/assembly': $jar")
        val tempDir = Files.createTempDirectory("cs441-emulator-hadoop-")
        val s3 = EmulatorS3.open()
        try
          val inputFile = tempDir.resolve("input.txt")
          s3.downloadFile(bucket, key, inputFile)
          val input = hadoop.newInputPath()
          val output = hadoop.newOutputPath()
          hadoop.putLocalFile(inputFile, input)
          hadoop.runScalaWordCount(jar, input, output)
          val outputText = hadoop.resultText(output)
          println("[hadoop result]")
          print(outputText)
          val resultsDir = hadoop.downloadResults(output, tempDir)
          val runId = s"job-${Instant.now().toEpochMilli}-${UUID.randomUUID().toString.take(8)}"
          val target = s3.uploadResults(resultsDir, bucket, outputPrefix, runId, output)
          println(s"[pipeline] PASS: results uploaded to $target")
        finally
          s3.close()
          // Temporary host-side payloads can contain source material: delete them.
          val files = Files.walk(tempDir)
          try files.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists(_))
          finally files.close()

      case _ =>
        println(usage)
        throw new IllegalArgumentException(s"Unrecognized command: ${args.mkString(" ")}")

  private def checkPureFunctions(): Unit =
    EmulatorS3.validateDestination("cs441-hadoop-staging-001", "inputs/sample.txt")
    val badBucket = try
      EmulatorS3.validateDestination("NOT_VALID", "sample.txt")
      false
    catch case _: IllegalArgumentException => true
    assert(badBucket)
    assert("Cloud Hadoop Cloud".toLowerCase(java.util.Locale.ROOT)
      .split("[^\\p{L}\\p{N}]+").filter(_.nonEmpty).toSeq ==
      Seq("cloud", "hadoop", "cloud"))
    println("[self-test] PASS: bucket/key validation and sample tokenization")
