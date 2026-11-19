package edu.cs441.hadoop.cluster

import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

/**
 * Manages REAL Hadoop daemons in Docker containers (HDFS and YARN).
 * AWS Emulator itself provides no arbitrary Hadoop JAR execution runtime.
 */
final class HadoopDocker(val moduleRoot: Path):
  private val dockerRoot = moduleRoot.resolve(".runtime/apache-hadoop-docker")
  private val projectName = "cs441-hadoop"
  private val tag = "docker-hadoop-3.4.2"
  // Verified official Apache Hadoop Docker branch revision, 2026-11-18.
  private val pinnedCommit = "0f4c272c0151e3e897b443d385aef6049603dbd4"
  private val hadoopArchive =
    "https://archive.apache.org/dist/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz"

  private def requireSetup(): Unit =
    require(Files.isRegularFile(dockerRoot.resolve("docker-compose.yaml")),
      "Hadoop Docker directory absent. Run 'setup' first.")

  private def compose(args: Seq[String], seconds: Long = 120L,
                      check: Boolean = true): CommandRunner.Result =
    requireSetup()
    CommandRunner.run(
      Seq("docker", "compose", "--project-name", projectName,
        "-f", dockerRoot.resolve("docker-compose.yaml").toString,
        "-f", moduleRoot.resolve("docker-compose.platform.yaml").toString) ++ args,
      dockerRoot, seconds, failOnNonzero = check
    )

  private def hdfs(args: Seq[String], seconds: Long = 120L,
                   check: Boolean = true): CommandRunner.Result =
    compose(Seq("exec", "-T", "namenode", "hdfs", "dfs") ++ args, seconds, check)

  private def hadoop(args: Seq[String], seconds: Long = 1200L): CommandRunner.Result =
    compose(Seq("exec", "-T", "namenode", "hadoop") ++ args, seconds)

  private def withUniqueSuffix(prefix: String): String =
    prefix + "-" + Instant.now().toEpochMilli + "-" + UUID.randomUUID().toString.take(8)

  private def safeHdfsPath(path: String): String =
    require(path.startsWith("/user/hadoop/cs441/"),
      s"HDFS paths must remain under /user/hadoop/cs441/: $path")
    require(!path.contains("..") && !path.contains("//") && !path.contains("\u0000"),
      s"Invalid HDFS path: $path")
    path

  def setup(): Unit =
    CommandRunner.run(Seq("git", "--version"), moduleRoot)
    CommandRunner.run(Seq("docker", "compose", "version"), moduleRoot)
    Files.createDirectories(dockerRoot.getParent)
    if !Files.exists(dockerRoot) then
      println("[setup] retrieving Apache Hadoop's published Docker configuration")
      CommandRunner.run(
        Seq("git", "clone", "--depth", "1", "--single-branch", "--branch", tag,
          "https://github.com/apache/hadoop.git", dockerRoot.toString),
        moduleRoot, 300L
      )
    val existing = CommandRunner.run(
      Seq("git", "-C", dockerRoot.toString, "rev-parse", "HEAD"), moduleRoot
    ).output.trim
    if existing != pinnedCommit then
      println(s"[setup] pinning Apache's Docker configuration to $pinnedCommit")
      CommandRunner.run(
        Seq("git", "-C", dockerRoot.toString, "fetch", "--depth", "1", "origin", pinnedCommit),
        moduleRoot, 300L
      )
      CommandRunner.run(
        Seq("git", "-C", dockerRoot.toString, "checkout", "--detach", pinnedCommit),
        moduleRoot
      )
    // Official compose file mounts test.sh; the pinned checkout omits that file.
    val testSh = dockerRoot.resolve("test.sh")
    if !Files.exists(testSh) then Files.writeString(testSh, "#!/bin/sh\nexit 0\n")

    println("[setup] building Apache Hadoop 3.4.2 Docker image (first build downloads Hadoop)")
    compose(Seq("build", "--build-arg", s"HADOOP_URL=$hadoopArchive"), 2400L)
    println("[setup] built. Next: up")

  def up(workers: Int = 2): Unit =
    require(workers >= 1 && workers <= 4, "Workers must be 1..4 for this local lab")
    compose(Seq("up", "-d", "--scale", s"datanode=$workers",
      "--scale", s"nodemanager=$workers"), 300L)
    println(s"[up] started Hadoop daemons; expecting $workers DataNodes and NodeManagers")
    waitUntilReady(workers)
    println("[up] HDFS http://localhost:9870 ; YARN http://localhost:8088")

  def down(): Unit =
    compose(Seq("down", "--remove-orphans"), 120L)
    println("[down] Hadoop containers stopped. Data stored only inside removed containers is lost.")

  def status(): Unit =
    println("[docker]")
    print(compose(Seq("ps"), 60L).output)
    println("[hdfs]")
    print(compose(Seq("exec", "-T", "namenode", "hdfs", "dfsadmin", "-report"), 60L).output)
    println("[yarn]")
    print(compose(Seq("exec", "-T", "resourcemanager", "yarn", "node", "-list"), 60L).output)

  private def waitUntilReady(workers: Int): Unit =
    val livePattern = "Live datanodes \\((\\d+)\\)".r
    val yarnPattern = "Total Nodes:\\s*(\\d+)".r
    for attempt <- 1 to 50 do
      val h = compose(Seq("exec", "-T", "namenode", "hdfs", "dfsadmin", "-report"),
        30L, check = false)
      val y = compose(Seq("exec", "-T", "resourcemanager", "yarn", "node", "-list"),
        30L, check = false)
      val live = livePattern.findFirstMatchIn(h.output).map(_.group(1).toInt).getOrElse(0)
      val nodes = yarnPattern.findFirstMatchIn(y.output).map(_.group(1).toInt).getOrElse(0)
      if h.exitCode == 0 && y.exitCode == 0 && live >= workers && nodes >= workers then
        println(s"[ready] HDFS live DataNodes=$live; YARN active NodeManagers=$nodes")
        return
      if attempt % 10 == 0 then
        println(s"[wait] check=$attempt, HDFS live=$live, YARN nodes=$nodes")
      Thread.sleep(3000)
    throw new IllegalStateException(
      "Hadoop not ready: inspect 'status' and 'docker compose logs' in the Docker checkout"
    )

  def hdfsExists(path: String): Boolean =
    hdfs(Seq("-test", "-e", safeHdfsPath(path)), 30L, check = false).exitCode == 0

  /** Stage host file into Hadoop HDFS, using docker cp followed by hdfs dfs -put. */
  def putLocalFile(hostFile: Path, hdfsDestination: String): Unit =
    val dest = safeHdfsPath(hdfsDestination)
    require(Files.isRegularFile(hostFile), s"Input file not found: $hostFile")
    val containerFile = "/tmp/" + withUniqueSuffix("cs441-input") + ".txt"
    compose(Seq("cp", hostFile.toAbsolutePath.toString, s"namenode:$containerFile"))
    hdfs(Seq("-mkdir", "-p", dest.substring(0, dest.lastIndexOf('/'))))
    hdfs(Seq("-put", "-f", containerFile, dest))
    println(s"[hdfs] staged ${hostFile.getFileName} -> $dest")

  def newInputPath(): String =
    s"/user/hadoop/cs441/input/${withUniqueSuffix("run")}/input.txt"

  def newOutputPath(): String =
    s"/user/hadoop/cs441/output/${withUniqueSuffix("run")}"

  /** Executes Hadoop's bundled, unmodified Java WordCount example on YARN. */
  def runBundledWordCount(input: String, output: String): Unit =
    val in = safeHdfsPath(input)
    val out = safeHdfsPath(output)
    require(hdfsExists(in), s"HDFS input does not exist: $in")
    require(!hdfsExists(out), s"HDFS output already exists: $out")
    val jar = "/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar"
    val result = hadoop(Seq("jar", jar, "wordcount", in, out))
    println(s"[hadoop] built-in WordCount succeeded, exit=${result.exitCode}")

  /** Executes our custom Scala 3 WordCount jar on actual Hadoop YARN. */
  def runScalaWordCount(hostJar: Path, input: String, output: String): Unit =
    runJar(hostJar, "edu.cs441.hadoop.jobs.WordCountJob", input, output)

  /** Accepts arbitrary MapReduce entry-point JAR, but runs it only in local Docker. */
  def runJar(hostJar: Path, mainClass: String, input: String, output: String): Unit =
    val in = safeHdfsPath(input)
    val out = safeHdfsPath(output)
    require(Files.isRegularFile(hostJar), s"Job JAR not found: $hostJar")
    require(mainClass.matches("[A-Za-z_][A-Za-z_0-9]*(\\.[A-Za-z_][A-Za-z_0-9]*)*"),
      s"Invalid fully qualified main class: $mainClass")
    require(hdfsExists(in), s"HDFS input does not exist: $in")
    require(!hdfsExists(out), s"HDFS output already exists: $out")
    val remoteJar = "/tmp/" + withUniqueSuffix("cs441-job") + ".jar"
    compose(Seq("cp", hostJar.toAbsolutePath.toString, s"namenode:$remoteJar"))
    val result = hadoop(Seq("jar", remoteJar, mainClass, in, out))
    println(s"[hadoop] completed: $mainClass, exit=${result.exitCode}")

  def resultText(output: String): String =
    val out = safeHdfsPath(output)
    require(hdfsExists(out), s"HDFS result directory not found: $out")
    hdfs(Seq("-cat", s"$out/part-*"), 90L).output

  /** Copy a complete HDFS output directory onto the host for S3 upload. */
  def downloadResults(output: String, hostDestination: Path): Path =
    val out = safeHdfsPath(output)
    require(Files.isDirectory(hostDestination), s"Destination must exist: $hostDestination")
    val containerPath = "/tmp/" + withUniqueSuffix("cs441-results")
    hdfs(Seq("-get", out, containerPath), 120L)
    val local = hostDestination.resolve("output")
    compose(Seq("cp", s"namenode:$containerPath", local.toString), 120L)
    require(Files.isDirectory(local), s"No result directory after docker cp: $local")
    local

  /** MapReduce result assertion: independent of Hadoop job implementation details. */
  def assertSampleCounts(result: String): Unit =
    val rows = result.linesIterator.filter(_.trim.nonEmpty).map { line =>
      val fields = line.split("\\s+")
      require(fields.length == 2, s"Unexpected Hadoop output row: $line")
      fields(0) -> fields(1).toInt
    }.toMap
    require(rows == Map("cloud" -> 3, "hadoop" -> 2, "scala" -> 1),
      s"WordCount mismatch: $rows")
    println(s"[assert] expected counts verified: ${rows.toSeq.sorted.mkString(", ")}")

object HadoopDocker:
  def locate(): HadoopDocker =
    val current = Paths.get("").toAbsolutePath.normalize()
    val candidates = Seq(
      sys.env.get("HADOOP_CLUSTER_ROOT").map(Paths.get(_)),
      Some(current.resolve("hadoop-cluster")),
      Some(current),
      Some(current.getParent.resolve("hadoop-cluster"))
    ).flatten
    val found = candidates.find(path => Files.isRegularFile(path.resolve("data/wordcount.txt")))
      .getOrElse(throw new IllegalStateException(
        "Cannot locate hadoop-cluster. Run sbt from repository root or set HADOOP_CLUSTER_ROOT."
      ))
    new HadoopDocker(found.toAbsolutePath.normalize())
