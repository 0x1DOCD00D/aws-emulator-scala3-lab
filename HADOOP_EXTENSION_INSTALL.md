# Executing Scala 3 Hadoop MapReduce Jobs on a Local Virtual Cluster

A complete installation, build, deployment, execution, verification, and troubleshooting manual for the Hadoop extension of `aws-emulator-scala3-lab`

Document scope: the additive `hadoop-cluster` extension supplied with the corrected Git patch, using Scala 3.9.0, Hadoop 3.4.2, JDK 21, sbt 1.x, and sbt-assembly 2.3.1.

Revision: November 2026. This manual is derived from the actual source in the corrected Hadoop extension, not from a hypothetical EMR implementation. Command output and execution traces explicitly labeled as expected or illustrative must be confirmed by executing the laboratory on the target computer. The example paths assume a macOS checkout at `/Users/drmark/Github/aws-emulator-scala3-lab`; adapt the home directory if necessary.

## Contents

Use the section links below to move directly to setup, IntelliJ configuration, deployment, or troubleshooting. The worked trace and the fully manual Docker procedure are included for readers who want to understand precisely what Hadoop executes and where.

- [0. Read This First: What Is Actually Emulated?](#0-read-this-first-what-is-actually-emulated)
- [1. Repository Layout and Responsibilities](#1-repository-layout-and-responsibilities)
- [2. Prerequisites, System Resources, and Safety Boundaries](#2-prerequisites-system-resources-and-safety-boundaries)
- [3. Installing the Additive Extension Safely](#3-installing-the-additive-extension-safely)
- [4. Understanding sbt's Two Build Products](#4-understanding-sbts-two-build-products)
- [5. Building the Apache Hadoop Docker Image with the Controller](#5-building-the-apache-hadoop-docker-image-with-the-controller)
- [6. Starting and Verifying the Virtual Cluster](#6-starting-and-verifying-the-virtual-cluster)
- [7. Smoke Test: Prove That Real Hadoop Can Run a Job](#7-smoke-test-prove-that-real-hadoop-can-run-a-job)
- [8. Compile, Assemble, and Inspect the Scala 3 MapReduce JAR Using sbt CLI](#8-compile-assemble-and-inspect-the-scala-3-mapreduce-jar-using-sbt-cli)
- [9. Deploy and Run the Scala 3 JAR with One Controller Command](#9-deploy-and-run-the-scala-3-jar-with-one-controller-command)
- [10. Manual Deployment: Stage an HDFS File, Submit an Existing JAR, Read Results](#10-manual-deployment-stage-an-hdfs-file-submit-an-existing-jar-read-results)
- [11. Source-Level Explanation: Mapper, Combiner, Reducer, and Driver](#11-source-level-explanation-mapper-combiner-reducer-and-driver)
- [12. Worked Execution Trace: From Two Input Lines to Two Reducer Files](#12-worked-execution-trace-from-two-input-lines-to-two-reducer-files)
- [13. Inspecting an Actual YARN Application](#13-inspecting-an-actual-yarn-application)
- [14. IntelliJ IDEA: Import and Configure the Full Multi-Project Build](#14-intellij-idea-import-and-configure-the-full-multi-project-build)
- [15. IntelliJ Method A: Execute Everything with sbt Task Configurations](#15-intellij-method-a-execute-everything-with-sbt-task-configurations)
- [16. IntelliJ Method B: Direct Application Run Configurations for HadoopClusterCli](#16-intellij-method-b-direct-application-run-configurations-for-hadoopclustercli)
- [17. IntelliJ Troubleshooting: sbt Import, Working Directory, and JAR Path](#17-intellij-troubleshooting-sbt-import-working-directory-and-jar-path)
- [18. Optional AWS Emulator Integration: S3 Input, Real YARN Compute, S3 Output](#18-optional-aws-emulator-integration-s3-input-real-yarn-compute-s3-output)
- [19. IntelliJ Configuration for AWS Emulator S3 Commands](#19-intellij-configuration-for-aws-emulator-s3-commands)
- [20. Fully Manual Docker Deployment Without HadoopClusterCli](#20-fully-manual-docker-deployment-without-hadoopclustercli)
- [21. Troubleshooting Matrix for Docker, sbt, MapReduce, IntelliJ, and AWS Emulator](#21-troubleshooting-matrix-for-docker-sbt-mapreduce-intellij-and-aws-emulator)
- [22. Repeated Execution, Cleanup, and Data Retention](#22-repeated-execution-cleanup-and-data-retention)
- [23. Experiments to Demonstrate Real Distributed Execution](#23-experiments-to-demonstrate-real-distributed-execution)
- [24. Suggested Reproducibility Record and Student Evidence](#24-suggested-reproducibility-record-and-student-evidence)
- [25. Quick Reference: Commands in the Required Order](#25-quick-reference-commands-in-the-required-order)
- [26. Frequently Asked Technical Questions](#26-frequently-asked-technical-questions)
- [27. Validation Limits and References](#27-validation-limits-and-references)

## 0. Read This First: What Is Actually Emulated?

This project contains two distinct environments that must not be confused. AWS Emulator provides selected local AWS API implementations, including S3 and portions of the Amazon EMR control plane; it does not provide an arbitrary Hadoop MapReduce JAR execution environment. The Hadoop extension therefore starts actual Apache Hadoop services inside Docker containers and uses these services to run the mapper and reducer. The resulting cluster is virtualized at the container level, not simulated by the AWS Emulator EMR implementation.

The phrase "virtual Hadoop cluster" means that a NameNode, one or more DataNodes, a YARN ResourceManager, and one or more YARN NodeManagers run as independent processes in containers on the developer's computer. These processes maintain real HDFS metadata, exchange data, schedule application containers, and execute tasks. However, they all share the host computer's underlying CPU, memory, storage, and physical failure domain. Consequently, the laboratory is appropriate for studying MapReduce execution semantics, job packaging, scheduling, data flow, and functional failures, but not for measuring production AWS EMR costs or multi-machine fault independence.

The project uses two Scala 3 sbt subprojects. The `hadoopJob` project compiles `WordCountJob.scala` and assembles the deployable JAR; the `hadoopCluster` project compiles `HadoopClusterCli.scala`, `HadoopDocker.scala`, `CommandRunner.scala`, and `EmulatorS3.scala`. The controller does not compile a JAR on demand: the JAR must already exist before invoking the `wordcount` or `s3-wordcount` commands. Running the job's `main` method directly with IntelliJ's Java launcher is also not equivalent to submitting the JAR to YARN.

The complete workflow is:

```text
macOS host
  |-- sbt hadoopJob/assembly
  |      -> hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar
  |
  |-- hadoopCluster / HadoopClusterCli
         |
         +-- Docker Compose: start actual Apache Hadoop containers
         |      |-- namenode: HDFS metadata and Hadoop submission client
         |      |-- datanode (1..4): HDFS block storage
         |      |-- resourcemanager: YARN resource allocation
         |      +-- nodemanager (1..4): YARN task/container execution
         |
         +-- docker compose cp: host input -> NameNode container
         +-- hdfs dfs -put: container file -> distributed HDFS path
         +-- docker compose cp: host JAR -> NameNode container
         +-- hadoop jar: submit application -> YARN
         +-- HDFS part-r-* files -> read/verify results
         |
         +-- optional AWS Emulator S3 staging over HTTPS 127.0.0.1:9297
                S3 input -> host temporary file -> HDFS -> YARN
                YARN result -> host temporary directory -> emulated S3
```

Use the following acceptance criteria when evaluating a run. The initial `self-test` must pass without Docker or AWS; the cluster readiness check must observe the expected number of live DataNodes and YARN NodeManagers; the built-in Hadoop WordCount test must produce the expected word counts; and the custom Scala 3 JAR must execute successfully on YARN. Only after those checks should the optional S3 pipeline be used. A reported EMR cluster ID from AWS Emulator alone does not satisfy any of these Hadoop execution criteria.

## 1. Repository Layout and Responsibilities

After the corrected additive patch is applied, the relevant files are:

```text
aws-emulator-scala3-lab/
|-- build.sbt                                  original AWS Emulator lab
|-- hadoop-cluster.sbt                        new sbt project definitions
|-- project/
|   |-- build.properties                      existing sbt launcher version
|   |-- plugins.sbt                           existing plugins (if any)
|   `-- 99-cs441-hadoop-assembly.sbt          sbt-assembly plugin version 2.3.1
|-- src/main/scala/                           original AWS Emulator examples
|-- scripts/
|   |-- emulator-env.sh                       local AWS SDK and CLI environment
|   `-- check-emulator.sh                     local emulator identity check
|-- HADOOP_EXTENSION_INSTALL.md              this installation and execution manual
`-- hadoop-cluster/
    |-- README.md                             module introduction
    |-- docker-compose.platform.yaml         force AMD64 platform for Apache images
    |-- data/wordcount.txt                    two-line deterministic test input
    |-- scripts/
    |   |-- preflight.sh                      executable/tool prerequisites
    |   `-- quickstart.sh                     automated happy-path commands
    |-- src/main/scala/edu/cs441/hadoop/cluster/
    |   |-- CommandRunner.scala               bounded child-process execution
    |   |-- HadoopDocker.scala                cluster, HDFS, and YARN orchestration
    |   |-- HadoopClusterCli.scala            controller entry point
    |   `-- EmulatorS3.scala                   local-only AWS Emulator S3 adapter
    |-- jobs/wordcount/
    |   `-- src/main/scala/edu/cs441/hadoop/jobs/
    |       `-- WordCountJob.scala             mapper, reducer, and job driver
    `-- .runtime/apache-hadoop-docker/        created by setup; local checkout
        |-- Dockerfile
        |-- docker-compose.yaml
        |-- config
        `-- test.sh                            auto-created compatibility file
```

The root `build.sbt` is deliberately left intact. sbt automatically reads additional root-level `.sbt` files, so `hadoop-cluster.sbt` can register new projects without modifying existing settings for `AwsEmulatorLab`. Similarly, the assembly plugin is loaded from `project/99-cs441-hadoop-assembly.sbt` without assuming that `project/plugins.sbt` is new. If the same sbt-assembly plugin has already been declared elsewhere, keep only one identical declaration to avoid plugin duplication.

In `hadoop-cluster.sbt`, the first project is named `hadoopCluster` and lives in `file("hadoop-cluster")`. It has the AWS SDK STS and S3 dependencies, because it is responsible for interacting with the local emulator when requested. The second project is named `hadoopJob` and lives in `file("hadoop-cluster/jobs/wordcount")`. Its Hadoop libraries are in `Provided` scope, because the Hadoop image supplies these libraries when the job executes.

The project uses Scala version `3.9.0` for both modules. The configured output JAR name is `cs441-wordcount.jar`, and the assembly task writes it under the `hadoopJob` target directory. In particular, the `HadoopClusterCli.wordCountJar` method constructs the full path `jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar` relative to `hadoop-cluster/`; this is why the Scala version and archive filename must match the actual sbt configuration. Changing `scalaVersion` without changing that hard-coded controller path will result in a missing-JAR error.

## 2. Prerequisites, System Resources, and Safety Boundaries

The local host needs a working JDK 21, sbt 1.x, Git, Docker Desktop with the Compose plugin, and sufficient disk space for a Hadoop image and dependency caches. On macOS, IntelliJ IDEA with the Scala plugin is recommended for editing, code navigation, and sbt integration, but it is not required for Terminal-only operation. If the optional S3 path is needed, AWS Emulator must also be running and must have generated the credentials and certificate files under `$HOME/.aws/emulator/9297`. The AWS CLI v2 is useful for independent S3 inspection and identity verification.

The published Apache `hadoop-runner` base image used by the pinned Dockerfile is intended for the AMD64 architecture. Accordingly, this module's Compose override specifies `platform: linux/amd64` for the NameNode, DataNode, ResourceManager, and NodeManager. On Apple Silicon, Docker Desktop must support x86-64 container emulation, which may impose substantial overhead on startup and job execution. Do not compare timings from this configuration with native ARM64 or production EC2 instances without explicitly accounting for the architecture difference.

A useful starting allocation is approximately 8 GB of memory to Docker Desktop and at least 15 GB of available disk space, although the actual requirements depend on the host, image layers, and local caches. If two worker containers cannot start reliably, first attempt a single DataNode and a single NodeManager using `up 1`. The controller explicitly accepts worker counts from 1 through 4. More containers do not necessarily produce faster results on one laptop, since the containers compete for the same hardware resources.

Check the toolchain in Terminal:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
java -version
sbt --version
git --version
docker --version
docker compose version
docker info
```

A successful `docker --version` is not sufficient: the Docker Desktop engine must also respond to `docker info`. If `docker info` cannot connect, launch Docker Desktop and confirm that it is operational. Verify that Java reports a version compatible with 21 and that sbt is launched from the project root, rather than from an unrelated directory. A project imported into IntelliJ may have a different project SDK, sbt launcher JRE, and Application JRE; all three should be checked when diagnosing version problems.

The provided preflight script consolidates these checks:

```bash
hadoop-cluster/scripts/preflight.sh
```

The script checks for `git`, `docker`, `java`, and `sbt`, asks Docker to report its engine state, and verifies Docker Compose availability. It does not build Hadoop, start containers, check the AWS Emulator, or prove that a MapReduce task can run. For that reason, a preflight success should be treated as a prerequisite check rather than as evidence of a complete deployment.

For S3-integrated executions, keep all traffic directed at the local emulator. The `EmulatorS3` adapter refuses an endpoint other than `https://127.0.0.1:9297`, demands the expected local generated credential file and PKCS12 trust store, and verifies that STS returns account `000000000000`. These restrictions deliberately prevent the sample pipeline from being redirected to a real AWS account by an accidental profile or environment change. Never disable those checks merely to work around an error, and do not use sensitive or production data in the emulator.

## 3. Installing the Additive Extension Safely

This manual assumes the corrected Hadoop extension patch from the prior project revision, not the original patch that attempted to recreate existing files. The corrected patch introduces `hadoop-cluster.sbt` and `project/99-cs441-hadoop-assembly.sbt` rather than editing or replacing the root `build.sbt`. It also adds new source and script files under `hadoop-cluster/`. Consequently, its installation does not depend on a particular line number inside your existing `build.sbt`.

If the extension has not yet been installed, run the following from the repository root, using the actual path to your downloaded corrected patch:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab

git status --short
git apply --check /path/to/hadoop-cluster-extension-fixed.patch
git apply /path/to/hadoop-cluster-extension-fixed.patch
```

Do not reapply this patch if the extension's files already exist. In particular, a message such as `hadoop-cluster.sbt: already exists` does not mean that the patch needs to be forced; it often means the extension is installed already. Avoid `git apply --reject`, broad directory deletion, or overwriting `build.sbt` to resolve such a conflict. If the project is already installed, skip directly to the verification commands below.

To replace the documentation alone with this rewritten manual, copy this file to the repository root:

```bash
cp /path/to/downloaded/HADOOP_EXTENSION_INSTALL.md \
  /Users/drmark/Github/aws-emulator-scala3-lab/HADOOP_EXTENSION_INSTALL.md
```

Verify that the essential files exist:

```bash
ls -l hadoop-cluster.sbt
ls -l project/99-cs441-hadoop-assembly.sbt
ls -l hadoop-cluster/src/main/scala/edu/cs441/hadoop/cluster/HadoopClusterCli.scala
ls -l hadoop-cluster/jobs/wordcount/src/main/scala/edu/cs441/hadoop/jobs/WordCountJob.scala
ls -l hadoop-cluster/docker-compose.platform.yaml
```

Next, inspect the sbt model without starting Hadoop:

```bash
sbt projects
sbt 'show hadoopCluster/scalaVersion' 'show hadoopJob/scalaVersion'
sbt 'hadoopCluster/compile' 'hadoopJob/compile'
```

The project listing should include `hadoopCluster` and `hadoopJob`, and both reported Scala versions should be `3.9.0`. On first compilation, sbt may download a large number of Scala, AWS SDK, and Hadoop artifacts; the download is separate from Docker's image build. If compilation fails, the first compiler error is generally more useful than the last line of a long stack trace. Do not proceed to `hadoopJob/assembly` until compilation completes.

Run the controller's source-level self-test:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli self-test'
```

The expected output contains:

```text
[self-test] PASS: bucket/key validation and sample tokenization
```

This test checks the input parser's sample tokenization and the S3 destination validator; it does not require Docker or the AWS Emulator. This separation is intentional: a failure in a pure Scala function can be diagnosed without also debugging container networking and AWS credentials.

## 4. Understanding sbt's Two Build Products

The `hadoopCluster` project produces the classes needed to run the local controller. Its main entry point is `edu.cs441.hadoop.cluster.HadoopClusterCli`, which accepts commands such as `setup`, `up`, `status`, `smoke`, `wordcount`, `stage`, `run-jar`, `show`, `s3-put`, and `s3-wordcount`. These commands are invoked from sbt using `hadoopCluster/runMain` and are run on the host JVM. The controller in turn launches Docker CLI processes through Java `ProcessBuilder` rather than directly executing Hadoop libraries inside the host JVM.

The `hadoopJob` project produces the MapReduce application intended to execute inside the Hadoop cluster. Its main class is `edu.cs441.hadoop.jobs.WordCountJob`, and it includes `WordCountMapper` and `WordCountReducer`. It depends on Hadoop API classes at compile time, but those Hadoop libraries are marked `Provided` for assembly so that the image's Hadoop runtime supplies them. In contrast, the assembled JAR includes the Scala runtime dependencies needed by the compiled Scala 3 classes.

The distinction between compilation and assembly is essential. `hadoopJob/compile` creates `.class` files under the job's target directory, while `hadoopJob/assembly` combines application classes with the required nonprovided dependencies into one deployable JAR. A successful IDE Build Project operation does not necessarily run sbt-assembly. Therefore, explicitly invoke the assembly task whenever the JAR has not been built, and invoke it again after changing the mapper, reducer, or driver.

The relevant sbt settings are:

```scala
lazy val hadoopJob = (project in file("hadoop-cluster/jobs/wordcount"))
  .settings(
    name := "cs441-hadoop-wordcount",
    scalaVersion := "3.9.0",
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-common" % "3.4.2" % Provided,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.2" % Provided
    ),
    assembly / mainClass := None,
    assembly / assemblyJarName := "cs441-wordcount.jar"
  )
```

This excerpt contains only the settings relevant to understanding packaging; the actual `hadoop-cluster.sbt` also contains a custom merge strategy and the controller project's dependency declarations. `assembly / mainClass := None` is not an error: the controller explicitly supplies the job's main class in the `hadoop jar` invocation. The custom assembly merge rules discard selected signature metadata and duplicate `module-info.class` entries to reduce archive collisions. If the build succeeds but YARN later reports a linkage or class-loading error, investigate the JAR contents and the precise Hadoop runtime classpath rather than assuming that assembly alone guarantees compatibility.

## 5. Building the Apache Hadoop Docker Image with the Controller

The `setup` command builds an Apache Hadoop installation inside Docker; it does not build the Scala application JAR. Internally, `HadoopDocker.setup()` clones the official Apache Hadoop repository's `docker-hadoop-3.4.2` branch into `hadoop-cluster/.runtime/apache-hadoop-docker`, then pins that checkout to commit `0f4c272c0151e3e897b443d385aef6049603dbd4`. The pinned revision matters because a branch can move independently of the laboratory's code. After checking out the revision, setup adds a local `test.sh` if necessary to satisfy the upstream Compose mount and asks Docker Compose to build the image using the Hadoop 3.4.2 archive URL.

Execute:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli setup'
```

The action has an extended process timeout because it may fetch a substantial archive and build many image layers on the first run. The output should identify the Apache Docker checkout and the Docker image build; on later runs, it reuses the checkout and Docker build cache when possible. If the command fails while downloading the Hadoop tarball, check network access and the official archive address before changing Scala code. If the command fails before Docker starts, check `docker compose version` and `docker info`.

The controller uses the equivalent Docker Compose operation:

```bash
cd hadoop-cluster/.runtime/apache-hadoop-docker

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml \
  -f ../../docker-compose.platform.yaml \
  build \
  --build-arg HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz
```

The first Compose file comes from Apache Hadoop and declares the Hadoop daemon services. The second file is supplied by the extension and sets `platform: linux/amd64` on the services; it does not replace Apache's service definitions. The Compose project name `cs441-hadoop` isolates this stack from unrelated projects that may also have services named `namenode` or `datanode`. You normally do not need to run the Docker command manually, but understanding it is useful when diagnosing a build problem outside IntelliJ.

The pinned Apache configuration provides a NameNode container, a scalable DataNode service, a ResourceManager container, and a scalable NodeManager service. It enables YARN as the MapReduce execution framework and the `mapreduce_shuffle` NodeManager auxiliary service. HDFS replication is configured to one for this lightweight laboratory, meaning that an HDFS block has a single replica by default. Consequently, losing the only DataNode holding a block can make that input unavailable; this is not the replication policy normally chosen for fault-tolerant production storage.

The Docker build and the Scala JAR build are separate, independent stages. A common error is to run `setup` and assume that `cs441-wordcount.jar` was generated: it was not. Conversely, successfully generating the Scala JAR does not start HDFS or YARN. The following sections explicitly test each layer before connecting them.

## 6. Starting and Verifying the Virtual Cluster

Start with two DataNodes and two NodeManagers:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli up'
```

To request a single worker instead, use:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli up 1'
```

For three workers, use `up 3`; the supported upper bound is four. The argument `workers` controls the number of DataNode containers and the number of NodeManager containers, but it does not increase the number of NameNodes or ResourceManagers. Thus, `up 2` ordinarily starts one NameNode, two DataNodes, one ResourceManager, and two NodeManagers. It is important to distinguish an HDFS DataNode from a YARN NodeManager: the former stores blocks, whereas the latter provides compute execution capacity. Docker schedules these services as separate containers, possibly sharing identical physical CPUs and disks.

The `up` implementation calls Docker Compose with `up -d --scale datanode=2 --scale nodemanager=2` and then enters a readiness loop. It runs `hdfs dfsadmin -report` in the NameNode container to find the number of live DataNodes and `yarn node -list` in the ResourceManager container to find active NodeManagers. Every three seconds it checks again until the requested counts have been observed or its retry budget is exhausted. This is a stronger readiness condition than merely observing that Docker container processes have started.

An illustrative successful readiness message is:

```text
[up] started Hadoop daemons; expecting 2 DataNodes and NodeManagers
[ready] HDFS live DataNodes=2; YARN active NodeManagers=2
[up] HDFS http://localhost:9870 ; YARN http://localhost:8088
```

These are expected messages, not output observed from your computer. To inspect the running services independently, execute:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli status'
```

The `status` command displays Docker Compose status, an HDFS DataNode report, and a YARN node list. A successful status report establishes that the Hadoop daemons are observable, but it still does not prove that an application can allocate containers and complete a job. Hadoop can accept a running DataNode while a job fails because of insufficient YARN resources, configuration errors, or an invalid JAR. Therefore, the next stage uses Hadoop's own example JAR as a baseline.

Open the following local interfaces in a browser:

- HDFS NameNode status and filesystem information: http://localhost:9870
- YARN ResourceManager and applications: http://localhost:8088

The NameNode interface should show live DataNodes and HDFS capacity information. The ResourceManager interface should show available NodeManagers and, after a job is submitted, an application record. These are real Hadoop service interfaces served by Docker containers and should not be confused with AWS Emulator's HTTPS API endpoint on port 9297. If a port is already occupied by another application, the Compose operation may fail until the conflicting process is stopped or the published port mapping is changed.

For lower-level diagnostics, use Docker Compose directly from the generated Apache checkout:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab/hadoop-cluster/.runtime/apache-hadoop-docker

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml \
  -f ../../docker-compose.platform.yaml \
  ps

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml \
  -f ../../docker-compose.platform.yaml \
  logs --tail 100 namenode datanode resourcemanager nodemanager
```

These direct commands are primarily for diagnostics; the intended student workflow is through `HadoopClusterCli`. In particular, do not delete or rebuild all containers as the first response to a job failure, because doing so can destroy the HDFS input and output needed to understand the failure. Inspect the logs and resource reports before deciding which layer needs to be restarted.

## 7. Smoke Test: Prove That Real Hadoop Can Run a Job

Before running Scala 3 code, use the bundled Java WordCount example distributed with Hadoop 3.4.2. This test isolates the Docker, HDFS, YARN, and Java MapReduce configuration from your custom JAR. The controller stages the repository's sample input and submits the standard Hadoop example JAR with the `wordcount` command. A successful result therefore demonstrates that the essential distributed execution path works independently of the Scala assembly process.

Run:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli smoke'
```

The sample `hadoop-cluster/data/wordcount.txt` contains exactly two lines:

```text
cloud hadoop cloud
hadoop scala cloud
```

The exact expected counts are:

```text
cloud   3
hadoop  2
scala   1
```

The sample text contains three occurrences of `cloud`, two occurrences of `hadoop`, and one occurrence of `scala`. Hadoop may print the rows in a different order, particularly if the job has multiple reducers; the important invariant is the mapping from word to count. The controller's `assertSampleCounts()` converts the output to a Scala map and compares it with `Map("cloud" -> 3, "hadoop" -> 2, "scala" -> 1)`. For the built-in smoke test, the assertion is mandatory, so a WordCount implementation returning unexpected counts cannot silently be reported as a pass.

The controller's smoke-test sequence is implemented directly in `HadoopClusterCli`:

```scala
case List("smoke") =>
  val input = hadoop.newInputPath()
  val output = hadoop.newOutputPath()
  hadoop.putLocalFile(hadoop.moduleRoot.resolve("data/wordcount.txt"), input)
  hadoop.runBundledWordCount(input, output)
  val text = hadoop.resultText(output)
  print(text)
  hadoop.assertSampleCounts(text)
  println(s"[smoke] PASS; HDFS output=$output")
```

First, `newInputPath` and `newOutputPath` construct unique names using a timestamp and a random UUID fragment. Next, `putLocalFile` copies the sample from macOS into the NameNode container and invokes `hdfs dfs -put` to store the input in HDFS. Then, `runBundledWordCount` checks that the input exists and the output directory does not, and it submits the Hadoop example JAR. Finally, `resultText` concatenates Hadoop's `part-*` output files and the controller verifies the counts before reporting success.

An illustrative final message is:

```text
[assert] expected counts verified: (cloud,3), (hadoop,2), (scala,1)
[smoke] PASS; HDFS output=/user/hadoop/cs441/output/run-<generated-suffix>
```

The suffix is generated at runtime and must not be copied literally into another command. Record the actual HDFS output path displayed by your execution if you want to inspect the same run later. A failure at this stage points to a Hadoop or Docker problem, not to Scala 3 JAR packaging, because the custom JAR has not yet been involved. That distinction makes the smoke test an especially useful diagnostic boundary.

## 8. Compile, Assemble, and Inspect the Scala 3 MapReduce JAR Using sbt CLI

The deployable application is `WordCountJob.scala`, with source path:

```text
hadoop-cluster/jobs/wordcount/src/main/scala/edu/cs441/hadoop/jobs/WordCountJob.scala
```

Its corresponding sbt subproject is `hadoopJob`, not `hadoopCluster`. The source defines a `WordCountMapper` class, a `WordCountReducer` class, and the `WordCountJob` object containing `main`. The mapper transforms each input line into `(word, 1)` pairs; the reducer sums all integer counts belonging to a word. The same reducer implementation is also registered as a combiner, which can reduce network traffic by partially aggregating counts before shuffle.

From the repository root, compile the job:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
sbt 'hadoopJob/compile'
```

Compilation checks the Scala source against the Hadoop 3.4.2 API declarations. It also checks that the Scala compiler and dependent artifacts are available and that the mapper and reducer methods override the expected Hadoop Java signatures. However, the compile task does not produce the final self-contained deployment artifact. After compilation succeeds, assemble the JAR:

```bash
sbt 'hadoopJob/assembly'
```

The expected JAR path is:

```text
/Users/drmark/Github/aws-emulator-scala3-lab/hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar
```

Verify its existence:

```bash
ls -lh hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar
```

Inspect the entries using the JDK `jar` tool:

```bash
jar tf hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar \
  | grep -E 'WordCount(Job|Mapper|Reducer)|scala3/library'
```

At a minimum, the archive should contain the application classes, including the class generated for `WordCountMapper`, the class generated for `WordCountReducer`, and the class or companion module generated for `WordCountJob`. Depending on the compiler's output naming conventions, the Scala object may be represented by both a class and a module class with a dollar-sign suffix. The artifact should also include the Scala runtime dependencies needed for execution, since the Hadoop container does not automatically provide Scala 3. Avoid assuming that the presence of every class in the archive proves runtime compatibility; the final test remains a real YARN execution.

For repeatable build inspection, record the file's SHA-256 digest:

```bash
shasum -a 256 hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar
```

This digest identifies the exact archive bytes tested. If the application is rebuilt with a new compiler version, modified source, or changed dependency set, the digest may change. Record it with the Hadoop Docker commit and sample data when comparing test results across machines or troubleshooting a regression. In particular, rebuilding the Scala JAR does not automatically update a previously copied JAR inside a running container; the controller copies it again whenever a new `runJar` operation is submitted.

If `hadoopJob/assembly` reports an unresolved sbt key, verify that `project/99-cs441-hadoop-assembly.sbt` is present and that IntelliJ or sbt has reloaded the project. If it reports duplicate assembly plugin declarations, retain a single declaration of the intended version. If it reports class-file merge conflicts, inspect the specific duplicate entries and the configured merge strategy rather than blindly discarding unrelated classes. Avoid adding Hadoop's entire provided runtime to the assembly just to eliminate a missing class without establishing which classloader is responsible.

## 9. Deploy and Run the Scala 3 JAR with One Controller Command

Once the Hadoop smoke test and JAR assembly both succeed, submit the custom job:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli wordcount hadoop-cluster/data/wordcount.txt'
```

The word `wordcount` following the main class is a controller subcommand; it is not the Hadoop JAR's main class. `HadoopClusterCli` resolves the supplied local input path, verifies that the assembled JAR exists, chooses unique HDFS input and output paths, and stages the input into HDFS. It then deploys the JAR to the NameNode container and calls Hadoop with the class `edu.cs441.hadoop.jobs.WordCountJob`, followed by the HDFS input and output paths. Only after Hadoop returns success does the controller read the HDFS result and report the completion marker.

The logic inside `HadoopDocker.runJar` is equivalent to:

```text
1. Validate local JAR file and main-class name.
2. Confirm that HDFS input exists.
3. Confirm that the HDFS output does not exist.
4. docker compose cp <host-jar> namenode:/tmp/cs441-job-<suffix>.jar
5. docker compose exec -T namenode hadoop jar \
     /tmp/cs441-job-<suffix>.jar \
     edu.cs441.hadoop.jobs.WordCountJob \
     <HDFS-input> <HDFS-output>
6. Check subprocess exit status.
7. Read <HDFS-output>/part-* and print the result.
```

The controller does not copy the JAR individually into every worker container. The `hadoop jar` command starts the Hadoop job submission process from the NameNode container, and Hadoop's submission machinery distributes the application resources to YARN execution containers as required. The exact task placements and the number of task attempts depend on Hadoop's scheduling decisions, input splitting, available resources, and failure conditions. Thus, the fact that the JAR is initially copied only into the NameNode container does not mean that all computation executes on that container.

For the supplied test file, the expected word counts remain three, two, and one. The Scala implementation uses two reduce tasks, so Hadoop normally produces two reducer output part files, such as `part-r-00000` and `part-r-00001`; their individual contents may differ based on partitioning. The controller reads all matching `part-*` files and checks the aggregate map for the named sample input. This avoids relying on which particular reducer emitted which word.

If successful, the controller displays a message of the following form:

```text
[hadoop] completed: edu.cs441.hadoop.jobs.WordCountJob, exit=0
cloud   3
hadoop  2
scala   1
[assert] expected counts verified: (cloud,3), (hadoop,2), (scala,1)
[wordcount] PASS; HDFS output=/user/hadoop/cs441/output/run-<generated-suffix>
```

The exact line order and generated path can differ from this illustration. An exit code of zero is necessary for the controller's success result, but not by itself sufficient to establish semantic correctness; the WordCount assertion checks the expected values. The MapReduce driver also calls `job.waitForCompletion(true)`, which waits for Hadoop to report the terminal application result. If the job fails, the driver exits with an error rather than publishing a fabricated successful result.

## 10. Manual Deployment: Stage an HDFS File, Submit an Existing JAR, Read Results

The previous command hides several deployment steps behind a single controller action. For teaching and debugging, it is useful to execute those steps individually, while retaining the controller's guardrails. This section assumes that the Hadoop cluster is running and that the Scala JAR has been built with `hadoopJob/assembly`. The input and output paths below are fixed for readability and are therefore intended for one experiment; use a new output name for each subsequent run.

Step 1: Stage a host file into HDFS:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab

sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli stage hadoop-cluster/data/wordcount.txt /user/hadoop/cs441/manual-input/input.txt'
```

The first argument after `stage` is an existing macOS file. The second is the desired fully qualified HDFS path, which must begin with `/user/hadoop/cs441/` because the controller limits writes to its teaching namespace. The controller copies the host file to a uniquely named temporary file in the NameNode container, creates the destination's HDFS parent directory, and uses `hdfs dfs -put -f` to place the bytes in HDFS. The temporary file in the container is not the HDFS object: the `-put` step is what actually writes through the Hadoop filesystem client.

Step 2: Inspect the staged data in HDFS. Change to the Apache checkout where Docker Compose operates:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab/hadoop-cluster/.runtime/apache-hadoop-docker

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hdfs dfs -ls /user/hadoop/cs441/manual-input

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hdfs dfs -cat /user/hadoop/cs441/manual-input/input.txt
```

The first command lists the HDFS directory's entries and the second reads the bytes through HDFS. Neither command reads the macOS host file directly, so successful output independently confirms that staging reached the filesystem. If the file is not found, inspect the immediately preceding `stage` command's exit status rather than blaming the JAR. A valid MapReduce application cannot process an input that was never placed in HDFS.

Step 3: Return to the repository root and submit the assembled JAR. The main class and both HDFS paths must be supplied explicitly:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab

sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli run-jar hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar edu.cs441.hadoop.jobs.WordCountJob /user/hadoop/cs441/manual-input/input.txt /user/hadoop/cs441/manual-output/run-001'
```

The controller verifies that the local archive exists, validates the main class, checks the HDFS input, and refuses to overwrite an existing HDFS output directory. It then copies the JAR to the NameNode container and uses the container's installed Hadoop CLI to execute `hadoop jar`. The class `edu.cs441.hadoop.jobs.WordCountJob` is the MapReduce job driver, not a mapper or reducer class; its `main` method constructs a `Job` and supplies all job configuration. Because this operation runs on actual Hadoop/YARN, a failure must be diagnosed using the Hadoop client's output and YARN application logs.

Step 4: Retrieve the results through the controller:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli show /user/hadoop/cs441/manual-output/run-001'
```

The `show` command reads the matching `part-*` files from the output directory and prints the resulting text. It does not delete the HDFS files, nor does it create any S3 objects. If this command works, both the task output and the NameNode's HDFS read path are functioning. For the sample input, check that the resulting map contains `cloud=3`, `hadoop=2`, and `scala=1`.

Step 5: Inspect the output directory and the reducer files directly:

```bash
cd hadoop-cluster/.runtime/apache-hadoop-docker

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hdfs dfs -ls /user/hadoop/cs441/manual-output/run-001

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hdfs dfs -cat '/user/hadoop/cs441/manual-output/run-001/part-*'
```

The `part-r-*` files represent reducer output partitions. Each reducer writes its own file, so no single file is guaranteed to contain every word, and the whole output is not necessarily globally sorted. The wildcard is passed inside the container's HDFS command rather than expanded by the macOS shell. The `_SUCCESS` marker, if produced, is metadata indicating successful job output completion, not an additional data record that the WordCount parser should count.

For a second run, choose an unused output directory such as `/user/hadoop/cs441/manual-output/run-002`. Hadoop deliberately refuses to run a job that would overwrite an existing output directory, because silent replacement could destroy a previous result. The convenience `wordcount` command avoids this situation by generating unique output names automatically. The explicit `run-jar` command requires the caller to manage the output name because it supports user-specified input and output paths.

The `run-jar` controller action currently assumes that the application's main class accepts exactly two positional arguments: one HDFS input path and one HDFS output path. It also assumes that textual result files matching `part-*` can be printed afterward. A pre-existing Hadoop application that uses a different argument convention, produces binary SequenceFiles, or requires additional command-line flags will need an adapter or an extended controller before it can use this exact subcommand. The application JAR can still be submitted directly with `hadoop jar` inside Docker, provided the developer manages its own arguments and HDFS paths.

## 11. Source-Level Explanation: Mapper, Combiner, Reducer, and Driver

The sample job is not a Scala collection operation running on the Mac. It implements Apache Hadoop's Java MapReduce interfaces from Scala 3, so Hadoop controls when and where its methods are invoked. The mapper is a class extending `Mapper[LongWritable, Text, Text, IntWritable]`, and the reducer extends `Reducer[Text, IntWritable, Text, IntWritable]`. This generic type sequence describes the input and output key/value types for each stage.

The mapper's first type parameter, `LongWritable`, represents the byte offset of an input line within a file split. The second, `Text`, is Hadoop's UTF-8 string wrapper for that input line; the mapper output key is another `Text` representing a normalized word. The mapper output value is `IntWritable`, a Hadoop serialization-friendly integer wrapper storing the count one. Hadoop uses these `Writable` representations to serialize intermediate records efficiently during map processing and shuffle.

The mapper implementation is:

```scala
class WordCountMapper extends Mapper[LongWritable, Text, Text, IntWritable]:
  override def map(key: LongWritable, value: Text, context: Context): Unit =
    val tokens = value.toString.toLowerCase(Locale.ROOT)
      .split("[^\\p{L}\\p{N}]+")
      .filter(_.nonEmpty)
    tokens.foreach(token => context.write(new Text(token), new IntWritable(1)))
```

First, the mapper converts the input line to a Scala/Java string and lowercases it using `Locale.ROOT`, avoiding locale-specific letter transformations. Next, the regular expression divides the text wherever it encounters one or more characters that are neither Unicode letters nor Unicode numbers. Empty tokens are filtered out, so repeated spaces and punctuation do not generate empty-word records. Finally, each token yields exactly one `(word, 1)` intermediate pair through Hadoop's `Context` object.

For example, the line `Cloud Hadoop Cloud!` produces the pairs `(cloud, 1)`, `(hadoop, 1)`, and `(cloud, 1)`. Repeated terms are intentionally not deduplicated: counting occurrences requires preserving each occurrence until aggregation. The mapper does not choose which reducer will receive a word; the MapReduce framework partitions, sorts, groups, and routes keys between map and reduce phases. Its output order is also not a global ordering guarantee.

The reducer is:

```scala
class WordCountReducer extends Reducer[Text, IntWritable, Text, IntWritable]:
  override def reduce(
      word: Text,
      counts: java.lang.Iterable[IntWritable],
      context: Context
  ): Unit =
    val iterator = counts.iterator()
    var total = 0L
    while iterator.hasNext do
      total += iterator.next().get().toLong
    require(total <= Int.MaxValue, s"Word count overflow for ${word.toString}")
    context.write(word, new IntWritable(total.toInt))
```

The reducer receives one normalized word and an iterable containing every partial count assigned to that word's reducer invocation. It accumulates the integers in a `Long`, which avoids intermediate `Int` overflow during addition, and checks whether the final sum can be represented by `IntWritable`. It then emits exactly one final record for that word. The reducer may receive raw mapper values or partial values produced by combiners; both are valid because summation is associative for the supported count range.

The project registers `WordCountReducer` as both the combiner and the reducer. A combiner is an optional local aggregation optimization that Hadoop may invoke zero, one, or several times before network shuffle; it must not be treated as a mandatory computation step. Since summing occurrences is associative and commutative when no overflow occurs, replacing the sequence `(cloud, 1), (cloud, 1)` with `(cloud, 2)` preserves the final count. In contrast, an algorithm relying on a particular invocation count or using a non-associative operation such as naive averaging would generally be unsafe as a combiner.

The job driver configures the MapReduce application:

```scala
val conf = new Configuration()
val job = Job.getInstance(conf, "cs441-scala3-wordcount")
job.setJarByClass(classOf[WordCountMapper])
job.setMapperClass(classOf[WordCountMapper])
job.setCombinerClass(classOf[WordCountReducer])
job.setReducerClass(classOf[WordCountReducer])
job.setMapOutputKeyClass(classOf[Text])
job.setMapOutputValueClass(classOf[IntWritable])
job.setOutputKeyClass(classOf[Text])
job.setOutputValueClass(classOf[IntWritable])
job.setNumReduceTasks(2)

FileInputFormat.addInputPath(job, new Path(args(0)))
FileOutputFormat.setOutputPath(job, new Path(args(1)))

if !job.waitForCompletion(true) then System.exit(1)
```

`Configuration` loads Hadoop environment settings, while `Job.getInstance` creates a client-side job configuration with a recognizable name. `setJarByClass` identifies the application JAR using a class known to belong to the job, and the following methods tell Hadoop which mapper, combiner, and reducer implementations to instantiate. The map-output and final-output declarations are separate because some algorithms use different intermediate and final data types. Finally, `setNumReduceTasks(2)` requests two reducer tasks, and the input/output formats identify the HDFS file paths supplied as the job's two command-line arguments.

The driver waits for the job to complete and treats a failed Hadoop result as a failed application execution. No explicit call to `System.exit(0)` is needed on the successful path; the Java program returns normally. The implementation does not capture a YARN application ID in a structured Scala return value, although Hadoop prints job and application information while executing. If a later experiment needs to query exact task attempts or save performance metrics, the controller would have to parse supported output or query the YARN API explicitly.

## 12. Worked Execution Trace: From Two Input Lines to Two Reducer Files

Consider the sample input:

```text
cloud hadoop cloud
hadoop scala cloud
```

Assume the file uses UTF-8, newline characters of one byte, the default Hadoop text input format, and enough resources for the job to complete. Under these assumptions, the first line begins at byte offset 0 and the second line at offset 19. For this small file, Hadoop may assign both lines to one map task, although the number of map tasks is an execution decision, not a property fixed by the source code. The example trace below explains the logical records independently of how many map attempts Hadoop actually schedules.

Stage A: input records. The input format supplies the mapper with a line offset and text value. The logical calls are `map(0, "cloud hadoop cloud")` and `map(19, "hadoop scala cloud")`. Each invocation is local to a map task and operates only on the line provided to it. The mapper does not scan every file in the directory or decide the input split boundaries.

Stage B: mapper emissions. The first line emits `(cloud,1)`, `(hadoop,1)`, `(cloud,1)`. The second emits `(hadoop,1)`, `(scala,1)`, `(cloud,1)`. Across both lines, the intermediate multiset contains six records. The word `cloud` appears three times, `hadoop` twice, and `scala` once.

Stage C: optional combiner. If the two lines are processed in a single mapper and a combiner is invoked, it might replace the six records with `(cloud,3)`, `(hadoop,2)`, and `(scala,1)`. If there are multiple maps, it might produce separate partial totals that are later combined by the reducer. A combiner may also be skipped entirely, in which case all six original values reach the shuffle logically. None of these alternatives changes the expected final counts.

Stage D: partition and shuffle. Because the driver requests two reducers, Hadoop assigns each word to one of two partitions, typically using the configured key partitioner. All values for a given word must reach the same reducer so that a final count can be computed. Hadoop transfers intermediate data as necessary, sorts keys within reducer inputs, and groups values that share a key. Network transfers and spill files depend on map placement, buffer sizes, input partitions, and available worker resources.

Stage E: reduce. One reducer may receive `cloud -> [1,1,1]`, and another may receive `hadoop -> [1,1]` and `scala -> [1]`; this is an illustration, not a guarantee of actual hash partition assignments. The reducer calculates the sums and emits `(cloud,3)`, `(hadoop,2)`, and `(scala,1)` across the partitions. The output format writes tab-separated text records to files whose names identify the reducer tasks. The resulting directory ordinarily includes `part-r-00000`, `part-r-00001`, and a success marker, with the exact per-file allocation depending on partitioning.

Stage F: result collection. The controller executes `hdfs dfs -cat` with a wildcard matching every `part-*` file and collects the text. `assertSampleCounts` parses nonempty lines as a word and integer count, builds a map, and compares it with the known expected map. The test is insensitive to row order and reducer-file order, which is appropriate for a distributed reduction. A passing assertion therefore establishes application-level semantic correctness for this sample in addition to job-level completion.

The core mathematical invariant can be stated without assuming any scheduling order. Let `V(w)` denote the multiset of counts emitted for word `w`, and let `C(w)` be the final count. Then `C(w) = sum of v over all v in V(w)`. For `cloud`, the multiset is `[1,1,1]`, giving `C(cloud) = 3`; for `hadoop`, it is `[1,1]`, giving `C(hadoop) = 2`. Associativity and commutativity of addition explain why the same result is obtained when values are combined locally, shuffled in different orders, and reduced on different workers.

The trace does not claim that a small input will engage multiple mapper tasks or produce measurable distributed speedup. To investigate scheduling or scalability, create larger files, vary the worker count, and use the YARN interface to observe actual map/reduce attempts. A two-line file is appropriate for correctness verification precisely because its expected result is simple and deterministic. It is not a meaningful benchmark of Hadoop's scalability.

## 13. Inspecting an Actual YARN Application

The ResourceManager at `http://localhost:8088` provides an independent view of jobs submitted to the Docker Hadoop cluster. After `smoke` or `wordcount` starts, open the UI and examine the application list. The job should have an application identifier, an application type, a state, and other fields exposed by the installed YARN version. Follow the application to see available execution details and task-attempt links.

For a command-line view, use the pinned Apache checkout:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab/hadoop-cluster/.runtime/apache-hadoop-docker

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T resourcemanager yarn application -list -appStates ALL
```

The listing may include previous applications that remain in YARN's history for the lifetime of the cluster. Record the ID of the application created by the current experiment rather than assuming it is the first or only entry. Depending on the installed configuration, additional details can be queried using commands of the form `yarn application -status <application-id>` and `yarn logs -applicationId <application-id>`. Some logs may not remain available after container cleanup, so gather evidence before stopping the cluster.

When reviewing YARN output, distinguish the application's state from individual task-attempt states. An application may retry a failed map attempt and still finish successfully, and the final application state alone does not tell you whether retries occurred. Similarly, the presence of two NodeManagers does not imply that both processed useful work on a two-line file. A useful experimental report should record the number of map tasks, reduce tasks, attempts, actual NodeManagers used, wall-clock execution time, and job input/output paths when the information is available.

To inspect HDFS capacity and live storage nodes, execute:

```bash
docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hdfs dfsadmin -report
```

To inspect YARN node capacity, execute:

```bash
docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T resourcemanager yarn node -list
```

These commands help separate storage readiness from compute readiness. A live HDFS block store with no active YARN NodeManagers can accept a file but cannot execute the MapReduce job as configured. Conversely, available YARN compute capacity does not compensate for missing HDFS input data. The controller's readiness condition checks both precisely because a Hadoop cluster requires both systems for this laboratory.

## 14. IntelliJ IDEA: Import and Configure the Full Multi-Project Build

IntelliJ IDEA should open the existing repository as one sbt build, rather than opening `hadoop-cluster/` or `jobs/wordcount/` as unrelated projects. This is necessary because the project IDs `hadoopCluster` and `hadoopJob` are defined in the root-level `hadoop-cluster.sbt` file. Importing only the module directory may leave IntelliJ unaware of those project definitions and their assembly plugin. Keep the root `build.sbt`, the original AWS Emulator lab, and both Hadoop subprojects in the same linked build.

Step 1: Open IntelliJ IDEA and choose File > Open. Select `/Users/drmark/Github/aws-emulator-scala3-lab`, the directory containing `build.sbt` and `hadoop-cluster.sbt`. When IntelliJ asks whether to trust the project, trust it only after verifying that it is your own checkout of the intended source. The IDE may offer to import or link the sbt build; select the sbt import rather than treating the directory as an arbitrary folder.

Step 2: Verify the Scala plugin and JDK settings. Open IntelliJ Settings and find the Scala plugin in the Plugins page, and enable or install it if needed. Next, use File > Project Structure to select JDK 21 as the Project SDK; the Java path shown in your previous local executions was `/Library/Java/JavaVirtualMachines/openjdk-21.jdk/Contents/Home`. Under Settings > Build, Execution, Deployment > Build Tools > sbt, select the appropriate JRE for the sbt process, preferably that same JDK 21 installation.

Step 3: Wait for the sbt project import to complete. If the extension was added after IntelliJ originally opened the repository, locate the sbt tool window and trigger Reload All sbt Projects or the corresponding reload action. IntelliJ must re-evaluate both the root `.sbt` files and the `project/` plugin definitions to discover the new projects and the `assembly` task. If the IDE shows only the original root module, reloading the build is the first corrective action; it is not necessary to recreate the IntelliJ project from scratch.

Step 4: Inspect the project structure. The sbt tool window should show the root project as well as the `hadoopCluster` and `hadoopJob` subprojects. The IDE's Project view should expose the controller sources under `hadoop-cluster/src/main/scala` and the job sources under `hadoop-cluster/jobs/wordcount/src/main/scala`. When opening `WordCountJob.scala`, IntelliJ should resolve `org.apache.hadoop.mapreduce.Mapper` and `Reducer` through the job project's dependencies. When opening `EmulatorS3.scala`, it should resolve AWS SDK STS and S3 classes through the controller project's dependencies.

Step 5: Enable sbt-driven compilation if IntelliJ's incremental compiler differs from the sbt build. In Settings > Build, Execution, Deployment > Build Tools > sbt, inspect the options to use the sbt shell for builds. Delegating builds to sbt can make the IDE's compilation behavior more closely match Terminal and CI. Regardless of that option, the actual deployment archive must still be built with `hadoopJob/assembly`, because ordinary IDE compilation does not imply archive assembly.

Step 6: Verify the imported build using the sbt shell, without creating any run configurations yet. Open the sbt tool window's shell and enter:

```text
projects
hadoopCluster/compile
hadoopJob/compile
hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli self-test
```

These lines are sbt-shell commands. Do not prefix them with `sbt` or wrap each command in single quotes inside the sbt shell; those conventions are for invoking sbt through the macOS Terminal. The expected self-test pass establishes that the IDE loaded the controller's Scala 3 classes. Only after this succeeds should you configure the Docker commands in the IDE.

For additional reference, consult the official IntelliJ IDEA documentation on sbt project import, the sbt shell, and sbt Task run configurations at https://www.jetbrains.com/help/idea/sbt-support.html and https://www.jetbrains.com/help/idea/run-debug-configuration-sbt-task.html. The exact placement of the sbt tool window and the names of some menu commands may vary slightly with IntelliJ release and platform. The underlying configuration requirements do not change: a linked sbt build, a compatible JDK, the correct subproject, and an appropriate working directory.

## 15. IntelliJ Method A: Execute Everything with sbt Task Configurations

Using sbt Task configurations is the recommended IDE workflow when the immediate objective is to build and submit a Hadoop job. An sbt Task configuration runs the specified sbt command and understands the project's task graph and dependencies. This avoids requiring IntelliJ's direct Application launcher to discover the correct Scala runtime classpath independently. It also gives a convenient record of exactly which build task produced the JAR.

Create the build configuration as follows:

1. Open Run > Edit Configurations.
2. Choose Add New Configuration, then sbt Task.
3. Name it `CS441 Hadoop - Assemble JAR`.
4. Set its Commands field to `hadoopJob/assembly`.
5. Set Working directory to `$ProjectFileDir$` or the absolute repository root.
6. Select the sbt shell option if desired, then apply the changes.
7. Run the new configuration and verify that `cs441-wordcount.jar` appears at its configured path.

The task's command is deliberately `hadoopJob/assembly`, not `hadoopCluster/assembly` and not a generic `compile`. The former builds the actual MapReduce artifact; the controller project has no responsibility for packaging Hadoop mapper and reducer classes. If the sbt Task configuration reports an unknown project or unknown task, first run `sbt projects` or the equivalent sbt shell command and verify the assembly plugin installation. The output path should be checked after a successful task because later execution relies on the existing file.

Create additional sbt Task configurations using the same procedure, changing only the configuration name and Commands field:

| IntelliJ sbt Task configuration name | Commands field |
| --- | --- |
| `CS441 Hadoop - Self Test` | `hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli self-test` |
| `CS441 Hadoop - Docker Setup` | `hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli setup` |
| `CS441 Hadoop - Start 2 Workers` | `hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli up 2` |
| `CS441 Hadoop - Status` | `hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli status` |
| `CS441 Hadoop - Built-in Smoke Test` | `hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli smoke` |
| `CS441 Hadoop - Run Scala WordCount` | `hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli wordcount hadoop-cluster/data/wordcount.txt` |
| `CS441 Hadoop - Stop Cluster` | `hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli down` |

All of these configurations should use the repository root as the working directory. A shared working directory makes the relative sample path `hadoop-cluster/data/wordcount.txt` predictable and lets `HadoopDocker.locate()` find the module. If IntelliJ starts a process from a different directory, the controller can alternatively use `HADOOP_CLUSTER_ROOT` to identify the absolute module path. However, the simplest configuration is to make the working directory correct rather than adding another required environment variable.

The intended execution order in IntelliJ is: Self Test, Docker Setup, Start 2 Workers, Built-in Smoke Test, Assemble JAR, Run Scala WordCount. The `setup` step is normally required only initially or when the Docker image must be rebuilt, whereas `assembly` is required whenever the deployable job changes. On a subsequent session, start Docker Desktop if necessary, check Status, and run the job against the existing cluster. The `down` action removes containers and can destroy HDFS data; do not use it before collecting output and logs.

It is also possible to launch the same tasks directly from IntelliJ's sbt shell:

```text
hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli setup
hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli up
hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli smoke
hadoopJob/assembly
hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli wordcount hadoop-cluster/data/wordcount.txt
```

This is operationally equivalent to running the quoted `sbt '...'` commands in Terminal, provided that the IDE's sbt process uses the intended JDK and project environment. A separately launched Terminal can inherit environment variables from `source scripts/emulator-env.sh`; the IntelliJ sbt shell generally will not inherit that terminal session's environment. That difference matters for optional S3 commands and is addressed in Section 19.

## 16. IntelliJ Method B: Direct Application Run Configurations for HadoopClusterCli

An Application configuration starts the controller's JVM main class directly using IntelliJ's selected module classpath. This can be convenient for debugging Scala logic, setting breakpoints in `HadoopDocker.scala`, and inspecting errors in `CommandRunner.scala`. However, unlike an sbt Task configuration, an Application configuration does not automatically run sbt-assembly before execution. It also does not automatically apply `Compile / run / javaOptions` settings from `hadoop-cluster.sbt`, because those settings govern an sbt-launched run, not an arbitrary Java invocation issued directly by IntelliJ.

Create a direct Application configuration:

1. Choose Run > Edit Configurations > Add New Configuration > Application.
2. Name it `CS441 Hadoop Controller - Smoke`.
3. Set Main class to `edu.cs441.hadoop.cluster.HadoopClusterCli`.
4. Select the module/classpath corresponding to the `hadoopCluster` sbt subproject.
5. Set the JRE to JDK 21.
6. Set Working directory to `$ProjectFileDir$` or `/Users/drmark/Github/aws-emulator-scala3-lab`.
7. Set Program arguments to `smoke`.
8. Leave AWS environment variables and emulator-specific VM options unset for this Docker-only command.
9. Apply and run the configuration after the cluster is ready.

For this direct mode, the selected classpath must be the controller project, because `HadoopClusterCli` belongs to `hadoopCluster`. Choosing the `hadoopJob` classpath may result in `ClassNotFoundException` for the controller or missing AWS SDK classes when using the S3 adapter. Conversely, running the job's own `WordCountJob` as an Application on the Mac bypasses the Docker YARN execution environment. The job should be packaged with `hadoopJob/assembly` and submitted using the controller or Hadoop CLI.

Create a second Application configuration for the custom Scala job by duplicating the Smoke configuration and changing Program arguments to:

```text
wordcount hadoop-cluster/data/wordcount.txt
```

Name it `CS441 Hadoop Controller - Scala WordCount`. Before running it for the first time, execute the sbt assembly task and verify the JAR file. After changing a mapper or reducer, rerun assembly before invoking this Application configuration; an IDE Build Project operation is not a replacement. The controller searches for the JAR at the configured path and explicitly reports `Build it first with sbt 'hadoopJob/assembly'` when the file is absent.

Create a third Application configuration for an existing JAR if desired. Set its Program arguments to the following single argument line, using a free output path:

```text
run-jar hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar edu.cs441.hadoop.jobs.WordCountJob /user/hadoop/cs441/manual-input/input.txt /user/hadoop/cs441/manual-output/idea-run-001
```

This configuration assumes that the manual input has already been staged in HDFS. If that condition is not satisfied, the controller will refuse to run rather than invent an input file. The complete call is a Hadoop job submission and output retrieval operation performed by the controller, not a direct invocation of the MapReduce driver's Java `main` in the host process. Its two HDFS paths are passed as application arguments after the main-class name.

The following field comparison is important when maintaining IntelliJ configurations:

| IntelliJ field | Docker-only controller | S3-integrated controller | JAR assembly |
| --- | --- | --- | --- |
| Configuration type | Application | Application | sbt Task |
| Main class | `edu.cs441.hadoop.cluster.HadoopClusterCli` | Same | Not applicable |
| Classpath/module | `hadoopCluster` | `hadoopCluster` | `hadoopJob` via task scope |
| Program arguments | `smoke` or `wordcount ...` | `s3-put ...` or `s3-wordcount ...` | Not applicable |
| sbt Commands field | Not applicable | Not applicable | `hadoopJob/assembly` |
| Working directory | Repository root | Repository root | Repository root |
| AWS environment variables | Not required | Required | Not required |
| Java TLS trust store | Not required | Required | Not required |
| JDK | 21 | 21 | Compatible sbt JDK, recommended 21 |

A direct Application configuration is particularly useful for breakpoints at the transition from HDFS staging to `runScalaWordCount()`. When debugging, remember that `CommandRunner` launches a Docker process and waits for its exit; stepping over that call does not step into remote Hadoop map or reduce tasks. To investigate the worker-side code, use task logs, a YARN-specific remote debugging procedure, or focused local unit tests for the mapper/reducer logic. Do not misinterpret a breakpoint in the host-side controller as a breakpoint in a NodeManager's JVM.

## 17. IntelliJ Troubleshooting: sbt Import, Working Directory, and JAR Path

If IntelliJ reports that `hadoopCluster` or `hadoopJob` does not exist, verify that `hadoop-cluster.sbt` is in the repository root, not inside the `hadoop-cluster/` folder. Reload the sbt project through the sbt tool window and check the project list from an external Terminal. A successful Terminal `sbt projects` combined with a failed IDE import usually indicates an IntelliJ linked-build or JDK configuration issue, not missing Scala source code. Conversely, if both Terminal and IntelliJ fail, inspect the sbt build definition and assembly plugin declarations.

If the sbt shell can compile the controller but a direct Application cannot find its main class, check the Application configuration's module classpath. It must refer to the imported `hadoopCluster` module rather than the root AWS Emulator lab module or `hadoopJob`. Check that the correct classes were produced by a successful compile operation and that the IDE did not select a stale project model. Reloading the build after adding new `.sbt` files is often necessary.

If the controller reports `Cannot locate hadoop-cluster`, inspect its Working directory. The implementation checks the current working directory, a `hadoop-cluster` subdirectory of that location, and the parent directory, and also honors the optional `HADOOP_CLUSTER_ROOT` environment variable. For the given checkout, set the Working directory to the repository root, `/Users/drmark/Github/aws-emulator-scala3-lab`. Setting the optional variable directly to `/Users/drmark/Github/aws-emulator-scala3-lab/hadoop-cluster` is an alternative, but it should not be needed in a correctly configured project.

If the controller reports a missing Scala JAR, verify the exact location, including the versioned `scala-3.9.0` directory. Rebuild using `hadoopJob/assembly` and check that the resulting name is `cs441-wordcount.jar`. If the file exists under a different Scala version directory or has a different assembly filename, the configured `wordCountJar()` path and build settings are inconsistent. Do not solve this by copying an old or unrelated JAR into the expected path: fix the configuration and rebuild the actual job.

If Docker commands work in macOS Terminal but fail from IntelliJ, verify that the IDEA process sees `docker`, `git`, and other executables on its PATH. Some GUI-launched macOS applications have a different inherited PATH than interactive shells. The controller runs the external command named `docker` with `ProcessBuilder`; it does not use an IntelliJ-integrated Docker plugin automatically. You can run the same sbt command in Terminal to distinguish a PATH problem from a Docker service problem.

If the IDE's sbt shell cannot use the correct Java version, inspect Settings > Build, Execution, Deployment > Build Tools > sbt > JRE separately from Project Structure > Project SDK. The IDE's Application configurations can also specify their own JRE, which introduces a third version choice. Consistent JDK 21 usage reduces class compatibility and TLS differences between Terminal and IDE runs. However, setting only the Project SDK does not guarantee that the sbt launcher and direct Application process use the same JDK.

## 18. Optional AWS Emulator Integration: S3 Input, Real YARN Compute, S3 Output

The local Hadoop cluster can run a MapReduce job without AWS Emulator, and that should remain the primary functional test. The optional S3 path introduces an additional input and output storage layer that behaves like an AWS API endpoint without creating a real AWS account resource. This integration does not convert the Docker cluster into an AWS EMR cluster: YARN still performs every mapper and reducer computation. The emulator supplies the local S3 service interface used by the staging application.

The pipeline is implemented by the `s3-wordcount` command and has three distinct storage locations. Input initially resides in an emulated S3 bucket, a temporary copy is downloaded onto the macOS filesystem, and the input is then written into HDFS. After MapReduce produces HDFS output, the controller copies the output to the host and uploads the reducer part files plus a manifest to emulated S3. These transfers are explicit and observable; the module does not silently mount S3 as an HDFS namespace or use Hadoop S3A internally.

Start AWS Emulator independently and verify that it has generated the endpoint credentials and certificate files. In a Terminal session, load the lab environment:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
source scripts/emulator-env.sh
scripts/check-emulator.sh
```

The environment script removes inherited real AWS credential variables and sets the endpoint to `https://127.0.0.1:9297`. It also sets `AWS_REGION=us-east-1`, the path to the emulator credentials, the AWS CLI certificate authority file, and `AWS_EMULATOR_JAVA_TRUSTSTORE`. The check script contacts the emulator health endpoint and calls STS `GetCallerIdentity`; the only acceptable account number is `000000000000`. If the identity differs, stop before any write operation and investigate the endpoint and credentials.

For reference, the expected emulator-specific environment values are:

| Variable | Expected value or file |
| --- | --- |
| `AWS_ENDPOINT_URL` | `https://127.0.0.1:9297` |
| `AWS_REGION` | `us-east-1` |
| `AWS_DEFAULT_REGION` | `us-east-1` |
| `AWS_SHARED_CREDENTIALS_FILE` | `$HOME/.aws/emulator/9297/emulator_credentials` |
| `AWS_CONFIG_FILE` | `$HOME/.aws/emulator/9297/config` |
| `AWS_CA_BUNDLE` | `$HOME/.aws/emulator/9297/ca.pem` |
| `AWS_EMULATOR_JAVA_TRUSTSTORE` | `$HOME/.aws/emulator/9297/truststore.p12` |
| `AWS_EC2_METADATA_DISABLED` | `true` |
| `AWS_DISABLE_HOST_PREFIX_INJECTION` | `true` |
| `AWS_PROFILE` and global AWS access-key variables | Must not be set |

The S3 adapter uses the AWS SDK for Java v2 from Scala 3. It constructs both an STS client and an S3 client with explicit endpoint overrides and `Region.US_EAST_1`; the S3 client also uses `forcePathStyle(true)`. Path-style access puts the bucket name in the request path instead of attempting to resolve a bucket-specific domain name, which is appropriate for a loopback-only endpoint. The adapter's validation does not rely exclusively on the environment variable: it explicitly constructs the local endpoint and checks the expected emulator account before performing S3 operations.

Run the first S3 operation from the same Terminal that sourced the environment script:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli s3-put hadoop-cluster/data/wordcount.txt cs441-hadoop-staging-001 inputs/wordcount.txt'
```

The `s3-put` subcommand accepts three arguments: a local file path, a bucket name, and an S3 object key. The example uses local file `hadoop-cluster/data/wordcount.txt`, bucket `cs441-hadoop-staging-001`, and key `inputs/wordcount.txt`. If the bucket does not exist, the adapter creates it; otherwise, it writes the specified key into the existing bucket. This operation does not run Hadoop or modify HDFS, so it can be used to verify S3 storage behavior in isolation.

Confirm the emulated object independently:

```bash
aws sts get-caller-identity --query Account --output text
aws s3 ls s3://cs441-hadoop-staging-001/inputs/
aws s3 cp s3://cs441-hadoop-staging-001/inputs/wordcount.txt -
```

The identity must be `000000000000`, and the object contents must match the two-line sample. The command-line AWS client relies on `AWS_ENDPOINT_URL` and `AWS_CA_BUNDLE` from the sourced environment. This is a separate client from the Scala AWS SDK and therefore provides a useful cross-check of the stored object. An S3 404 during this step indicates that the bucket or key is absent or that the session is not connected to the intended emulator state.

Next, ensure Hadoop is running and the custom Scala JAR exists:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli status'
sbt 'hadoopJob/assembly'
```

Run the complete pipeline:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli s3-wordcount cs441-hadoop-staging-001 inputs/wordcount.txt results/wordcount'
```

The three arguments to `s3-wordcount` are the source bucket, the source object key, and the output prefix. Here the output prefix is `results/wordcount`, not a complete name for any one result file. The program appends a unique run identifier and then uploads the reducer part files beneath the resulting path. The generated run ID has the form `job-<epoch-milliseconds>-<random-suffix>`, so consecutive executions should not intentionally overwrite each other's output prefixes.

The `s3-wordcount` controller performs the following sequence in its actual source code:

```text
1. Locate and validate the assembled cs441-wordcount.jar.
2. Create a temporary host-side directory.
3. Open EmulatorS3; validate endpoint, Region, files and trust store.
4. Call STS and require account 000000000000.
5. Download s3://<bucket>/<key> to the host temporary input.txt.
6. Generate unique HDFS input and output paths.
7. Copy input to NameNode container and stage it into HDFS.
8. Copy assembled JAR to NameNode container and submit hadoop jar.
9. Wait for real YARN job completion and read HDFS output.
10. Copy the complete HDFS output directory to the host.
11. Create a unique S3 result prefix with a run ID.
12. Upload every regular part-* file under that prefix.
13. Write and upload manifest.json.
14. Close AWS clients and delete host-side temporary files.
15. Report the target S3 URI only if the pipeline reaches success.
```

The pipeline does not delete the HDFS input directory, HDFS output directory, or emulated S3 objects after completion. It cleans up host-side temporary files in a `finally` block; this is a best-effort local cleanup measure rather than a persistent distributed retention policy. If execution fails between the MapReduce completion and S3 upload, Hadoop output may already exist while only some S3 result files were uploaded. The current implementation is therefore not an atomic cross-service transaction and requires a retry or reconciliation policy for production-style reliability experiments.

An example result prefix is:

```text
s3://cs441-hadoop-staging-001/results/wordcount/job-<generated-id>/
```

The directory typically contains reducer part files and `manifest.json`. The manifest records `computeEnvironment` as `Docker Hadoop 3.4.2 / real YARN, not emulated EMR`, `storageEnvironment` as `AWS Emulator S3`, the emulator account number, the recording timestamp, the HDFS output path, and the count of uploaded result part files. It does not currently contain a JAR SHA-256 digest, input checksum, worker count, or elapsed duration. Add these fields to the implementation if stronger experiment provenance is required.

Inspect the uploaded objects with:

```bash
aws s3 ls s3://cs441-hadoop-staging-001/results/wordcount/ --recursive
```

Use the exact generated prefix printed by your run when retrieving its manifest and reducer parts. Do not substitute the literal `<generated-id>` placeholder into an S3 command. Comparing the downloaded reducer contents against the HDFS outputs provides an end-to-end check that storage staging preserved the computation result. A successful upload establishes S3 API behavior only for those operations and does not establish correctness of real AWS authorization policies or production cloud availability.

## 19. IntelliJ Configuration for AWS Emulator S3 Commands

IntelliJ's direct Java launcher does not inherit variables from a Terminal session where `source scripts/emulator-env.sh` was executed unless IntelliJ itself was launched from that environment. It also does not automatically apply the sbt-only `Compile / run / javaOptions` settings. Consequently, the S3-integrated Application configuration needs its own process environment and JVM TLS settings. This is the same distinction responsible for the `AWS_ENDPOINT_URL` and trust manager errors encountered earlier in the original AWS Emulator laboratory.

First, duplicate the working `CS441 Hadoop Controller - Smoke` Application configuration from Section 16. Name the new configuration `CS441 Hadoop Controller - S3 Pipeline` and retain its Main class `edu.cs441.hadoop.cluster.HadoopClusterCli`, its `hadoopCluster` classpath, JDK 21, and repository-root working directory. Set Program arguments to:

```text
s3-wordcount cs441-hadoop-staging-001 inputs/wordcount.txt results/wordcount
```

In Run > Edit Configurations, locate the Environment variables field and add the following variables. Use absolute macOS paths in the IntelliJ configuration; do not assume that `$HOME` will be expanded when pasted into a field.

```text
AWS_ENDPOINT_URL=https://127.0.0.1:9297
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
AWS_SHARED_CREDENTIALS_FILE=/Users/drmark/.aws/emulator/9297/emulator_credentials
AWS_CONFIG_FILE=/Users/drmark/.aws/emulator/9297/config
AWS_CA_BUNDLE=/Users/drmark/.aws/emulator/9297/ca.pem
AWS_EC2_METADATA_DISABLED=true
AWS_DISABLE_HOST_PREFIX_INJECTION=true
AWS_EMULATOR_JAVA_TRUSTSTORE=/Users/drmark/.aws/emulator/9297/truststore.p12
```

Ensure that `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` are not defined in the Application configuration or inherited from a global IDEA environment. The `EmulatorS3` adapter explicitly rejects those variables to reduce the risk of using unintended credentials. The module does not accept real AWS endpoints as substitutes. Before running this configuration, verify that the generated emulator credential files still exist and that the emulator process that created them is currently running.

Open the VM Options field. If it is hidden, use Modify Options > Add VM Options. Enter exactly two JVM properties, separated by whitespace:

```text
-Djavax.net.ssl.trustStore=/Users/drmark/.aws/emulator/9297/truststore.p12
-Djavax.net.ssl.trustStoreType=PKCS12
```

In the actual IntelliJ field, the two properties may be entered as one line separated by a space. They must remain two distinct JVM options; specifically, the value of `javax.net.ssl.trustStoreType` must be exactly `PKCS12`. An earlier failed run passed the malformed property `-Djavax.net.ssl.trustStoreType=PKCS12/Users/drmark/.m2/repository`, which caused a Java TLS trust manager initialization failure. An unrelated directory path must never be appended to the store type.

To verify the direct Application configuration, insert these temporary diagnostics at the beginning of the controller's main method if debugging is necessary:

```scala
println("Endpoint: " + sys.env.getOrElse("AWS_ENDPOINT_URL", "(unset)"))
println("Trust store: " + System.getProperty("javax.net.ssl.trustStore"))
println("Trust store type: " + System.getProperty("javax.net.ssl.trustStoreType"))
```

Do not print real access keys or session tokens. For this lab, the expected endpoint is `https://127.0.0.1:9297`, the trust store path ends in `truststore.p12`, and the store type is exactly `PKCS12`. After diagnosis, remove temporary logging if it is not needed. The S3 adapter already performs its own strict runtime checks before opening network operations.

If you execute the S3 controller through an IntelliJ sbt Task instead of a direct Application, the environment must be available to the sbt process. The `hadoop-cluster.sbt` file adds the Java trust store properties to forked `Compile / run` processes when the sbt process sees `AWS_EMULATOR_JAVA_TRUSTSTORE`. Configure the sbt process environment in IntelliJ's sbt settings or start sbt from an appropriately configured Terminal. Do not rely on the VM Options field for an unrelated Application configuration to configure a separate sbt process.

The S3 upload task can be added as an IntelliJ sbt Task with Commands:

```text
hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli s3-put hadoop-cluster/data/wordcount.txt cs441-hadoop-staging-001 inputs/wordcount.txt
```

The S3 job task uses Commands:

```text
hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli s3-wordcount cs441-hadoop-staging-001 inputs/wordcount.txt results/wordcount
```

The IDE sbt shell must have the same emulator environment as Terminal for these tasks to work. Running a non-S3 controller command is useful for confirming that Docker and Hadoop are healthy before diagnosing an emulator TLS or credential issue. If `smoke` passes but `s3-put` fails, focus on the endpoint, identity, and certificate settings. If `s3-put` succeeds but `s3-wordcount` fails, inspect the Hadoop job, local temporary staging, and result-upload steps in that order.

## 20. Fully Manual Docker Deployment Without HadoopClusterCli

The controller is convenient, but it is not a required component of Hadoop. A useful way to understand the actual execution boundary is to deploy the JAR using only Docker Compose and Hadoop commands. This procedure also helps isolate defects in the controller from defects in the JAR or the Hadoop runtime. It assumes that the `setup` and `up` steps have succeeded, the JAR has been assembled, and the NameNode container is healthy.

First, build the artifact using sbt from the repository root:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
sbt 'hadoopJob/clean' 'hadoopJob/assembly'
ls -lh hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar
```

The `clean` step is optional, but in a reproducibility exercise it removes earlier `hadoopJob` build outputs before assembly. It does not stop Docker or remove HDFS contents. The exact JAR should be present before any deployment command; otherwise, there is nothing for Hadoop to submit. Record its SHA-256 digest if the experiment will be repeated or compared.

Second, switch to the local Apache Docker checkout:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab/hadoop-cluster/.runtime/apache-hadoop-docker
```

Third, copy the input file and JAR from the macOS filesystem into the NameNode container:

```bash
docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  cp /Users/drmark/Github/aws-emulator-scala3-lab/hadoop-cluster/data/wordcount.txt \
  namenode:/tmp/wordcount.txt

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  cp /Users/drmark/Github/aws-emulator-scala3-lab/hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar \
  namenode:/tmp/cs441-wordcount.jar
```

Fourth, create an HDFS input directory and copy the file from the container's local filesystem into HDFS:

```bash
docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hdfs dfs -mkdir -p /user/hadoop/cs441/direct-input

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hdfs dfs -put -f /tmp/wordcount.txt \
  /user/hadoop/cs441/direct-input/input.txt
```

The first `docker compose cp` operations write ordinary files into a container. By contrast, the `hdfs dfs -put` operation invokes Hadoop's filesystem client and writes the data under the HDFS namespace. This distinction explains why `docker compose cp` alone is insufficient for a MapReduce input that the job opens using `FileInputFormat`. You can verify the input with `hdfs dfs -cat` before submitting the job.

Fifth, submit the deployed JAR to Hadoop. Use a new output directory that does not exist:

```bash
docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hadoop jar /tmp/cs441-wordcount.jar \
  edu.cs441.hadoop.jobs.WordCountJob \
  /user/hadoop/cs441/direct-input/input.txt \
  /user/hadoop/cs441/direct-output/run-001
```

The command `hadoop jar` is executed inside the NameNode container, where Hadoop's command-line client and configuration are installed. It loads the job's driver, constructs the MapReduce job configuration, submits the application to the ResourceManager, and waits for completion. The ResourceManager allocates application and task containers on available NodeManagers, while the job's input and output are managed through HDFS. If there is insufficient YARN capacity or a class-loading failure, the command fails and the relevant messages should be visible in Hadoop or YARN logs.

Sixth, verify the output:

```bash
docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hdfs dfs -ls /user/hadoop/cs441/direct-output/run-001

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  exec -T namenode hdfs dfs -cat '/user/hadoop/cs441/direct-output/run-001/part-*'
```

This manual workflow deliberately leaves both container-local files and HDFS data in place, allowing the developer to inspect or clean them up after verification. On the next run, either use a fresh output directory or explicitly remove the old test output with the HDFS command after confirming it is disposable. The controller's automatic unique path generation prevents this collision in normal demonstrations. The manual route is particularly valuable for explaining which layer owns each artifact: host filesystem, Docker container filesystem, HDFS, and YARN execution metadata.

## 21. Troubleshooting Matrix for Docker, sbt, MapReduce, IntelliJ, and AWS Emulator

The most effective troubleshooting method is to identify the first layer that failed. A project may compile successfully while its Docker engine is unavailable, and a cluster may be healthy while the custom JAR is missing Scala runtime classes. Similarly, a MapReduce job can complete correctly while a subsequent S3 upload fails due to an unrelated HTTPS configuration problem. The table below maps specific symptoms to their likely failure boundary and the shortest defensible diagnostic action.

| Symptom | Failure boundary | Diagnostic and corrective action |
| --- | --- | --- |
| `docker: command not found` | Host PATH | Install Docker Desktop and make Docker CLI available to Terminal or IDEA. |
| `Cannot connect to the Docker daemon` | Docker engine | Start Docker Desktop; verify with `docker info`. |
| `hadoop-cluster` directory cannot be located | Working directory | Use repository root or set `HADOOP_CLUSTER_ROOT` to module root. |
| `Hadoop Docker directory absent` | Unperformed setup | Run `HadoopClusterCli setup` before `up`. |
| Docker image pull/build fails | Container/image/network | Inspect build output, internet access, disk capacity, and Apache archive URL. |
| AMD64 image error on Apple Silicon | Container architecture | Ensure the included platform override is used and Docker can emulate AMD64. |
| Docker port 9870 or 8088 already used | Local network binding | Stop conflicting service or deliberately change published port mappings. |
| `Hadoop not ready` | HDFS or YARN startup | Inspect `status`, Docker logs, worker memory, and readiness reports; retry with `up 1`. |
| `Live datanodes (0)` | HDFS storage | Inspect NameNode/DataNode logs and network registration. |
| `Total Nodes: 0` | YARN compute | Inspect ResourceManager/NodeManager logs and configured resource limits. |
| Built-in `smoke` fails | Hadoop runtime | Check HDFS/YARN first; custom Scala JAR is not involved yet. |
| sbt unknown `hadoopJob` | sbt import | Verify root-level `hadoop-cluster.sbt`, reload sbt project. |
| sbt unknown `assembly` | sbt plugin | Verify `project/99-cs441-hadoop-assembly.sbt` and reload. |
| Duplicate sbt-assembly plugin | sbt project definition | Keep a single plugin declaration for the intended version. |
| `Build it first with sbt 'hadoopJob/assembly'` | Artifact missing | Assemble JAR and verify versioned path and filename. |
| `ClassNotFoundException` for Hadoop controller in IDEA | Application module classpath | Select imported `hadoopCluster` module. |
| `NoClassDefFoundError` or `ClassNotFoundException` inside YARN | Job JAR or Hadoop classpath | Check assembled contents, Scala runtime inclusion, Hadoop `Provided` scope, task logs. |
| `NoSuchMethodError` / `IncompatibleClassChangeError` | Dependency binary mismatch | Compare compile-time Hadoop/Scala dependencies with actual worker runtime. |
| `HDFS input does not exist` | Data staging | Run `stage`, inspect `hdfs dfs -ls`, and use exact input path. |
| `HDFS output already exists` | Output management | Choose a fresh run-specific path; never overwrite silently. |
| YARN application remains ACCEPTED or fails to allocate | Scheduler/resources | Inspect YARN UI, NodeManager resources, and application diagnostics. |
| Wrong WordCount counts | Job semantics/input bytes | Inspect exact input content, mapper tokenization, and reducer outputs. |
| `AWS_ENDPOINT_URL` missing | Emulator environment | Source `scripts/emulator-env.sh` in the same Terminal or configure IDEA variables. |
| `No X509TrustManager implementation available` | JVM TLS trust store | Correct the PKCS12 VM properties and verify truststore file. |
| STS account is not `000000000000` | Endpoint/identity boundary | Stop immediately; use only intended local emulator credentials. |
| S3 `NoSuchKey` or 404 | Emulated storage | Check bucket/key with AWS CLI and run `s3-put` if needed. |
| S3 result prefix absent but Hadoop output present | Upload/staging | Inspect S3 adapter logs, credentials, and local temporary transfer stage. |

Begin with the fastest independent test. If `self-test` fails, there is no point investigating Hadoop; if `self-test` passes but `smoke` fails, investigate Docker, HDFS, and YARN before rebuilding the Scala JAR. If `smoke` passes but the custom WordCount job fails, inspect the assembly and the job classes. If the custom job succeeds but `s3-wordcount` fails, concentrate on the emulator connection or data-transfer stage.

The original AWS Emulator lab had three characteristic failures that are worth distinguishing from Hadoop errors. The first was `IllegalStateException: Set AWS_ENDPOINT_URL to the local emulator`, caused by an IntelliJ process missing the expected environment variable. The second was `SSLHandshakeException` with `No X509TrustManager implementation available`, caused by a malformed VM property where `PKCS12` was accidentally concatenated with an unrelated directory path. The third was `DynamoDB ResourceInUseException: Table already exists: CS441Ingest`, caused by rerunning a fixed-name table-creation operation against retained emulator state. These issues concern the original AWS API program and do not indicate that a Hadoop mapper or reducer is defective.

When an SDK operation fails over HTTPS, inspect the earliest causal exception rather than only the number of retry attempts. A certificate initialization failure can be retried several times without becoming a transient network problem. A missing endpoint configuration is a local program precondition failure, not a service outage. A `ResourceInUseException` on table creation may indicate expected persistent state and should be resolved with deliberate resource lifecycle management rather than retrying blindly.

To inspect container logs after a failed job, run:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab/hadoop-cluster/.runtime/apache-hadoop-docker

docker compose --project-name cs441-hadoop \
  -f docker-compose.yaml -f ../../docker-compose.platform.yaml \
  logs --tail 150 namenode datanode resourcemanager nodemanager
```

To inspect the application specifically, obtain its actual ID from the YARN UI or `yarn application -list -appStates ALL`. Then use `yarn application -status` and, when available, `yarn logs -applicationId` against that ID. Do not infer success merely because the ResourceManager interface is reachable or the container list shows `running`. Hadoop's application-level result and the WordCount semantic assertion are the final criteria for this sample.

## 22. Repeated Execution, Cleanup, and Data Retention

The convenience commands generate unique HDFS input and output paths, making them suitable for repeated experiments in the same running cluster. The explicit `run-jar` command accepts fixed paths and therefore requires the operator to select a fresh output path. The emulated S3 results also use a unique run-specific prefix rather than overwriting a previous result under the same key. These conventions reduce accidental collisions but do not implement global transaction semantics or automatic durable recovery.

The `down` command stops and removes the project's Docker containers:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli down'
```

In the supplied Apache Compose configuration, HDFS data may be stored inside disposable container filesystems. Removing those containers can therefore destroy staged HDFS input, intermediate files, and job outputs. The controller deliberately does not delete emulated S3 objects as part of `down`. Nevertheless, AWS Emulator stores its own state locally and that state may disappear when the emulator process is reset, so neither environment should be treated as a durable production backup.

To inspect the output before cleanup, use the `show` command or direct `hdfs dfs -ls` and `hdfs dfs -cat` commands. If you need results to survive a cluster teardown, copy them to a host directory or to a deliberately retained S3 destination first. For classroom data, it is reasonable to keep records of the input, command, build artifact digest, output, and application logs under a named experiment directory on the host. For sensitive data, design explicit permissions, a retention period, and a secure cleanup procedure rather than relying on container deletion.

Do not run indiscriminate Docker cleanup commands such as `docker system prune --all --volumes` as part of this laboratory. Such commands can delete unrelated images, containers, and volumes used by other projects. The named Compose project `cs441-hadoop` was chosen to support targeted management of the Hadoop environment. Likewise, limit HDFS deletion commands to the disposable `/user/hadoop/cs441/` namespace and inspect paths before removing anything.

The original AWS Emulator lab's fixed DynamoDB table name remains an independent rerun issue. Restarting the AWS Emulator or deleting only the disposable `CS441Ingest` table can address the table creation conflict, but the Hadoop extension does not depend on that table and should not destroy it automatically. The MapReduce sample itself uses unique HDFS paths and does not require resetting AWS Emulator between ordinary Docker-only runs. Keeping the resource lifecycle boundaries separate makes the experiments safer and easier to interpret.

## 23. Experiments to Demonstrate Real Distributed Execution

After the basic application passes, the next step is to vary one factor at a time while recording observable evidence. For example, run the job with one NodeManager and then with two while keeping the input and JAR unchanged. Compare YARN application histories, task placements, completion times, and resource usage. Because all containers share one host, any observed speedup or slowdown is a property of this local configuration and workload, not an estimate of how a commercial EMR cluster would behave.

For a larger experiment, construct a reproducible input file with a known number of repeated words and record its SHA-256 digest before staging. Ensure that the input is large enough to exercise multiple file splits and actual scheduling decisions; the two-line smoke test is too small for performance claims. Run several repetitions and report the mean, variability, and full set of observations rather than a single best timing. Keep the Docker worker count, application JAR, JVM, and input file fixed within each comparison group.

A useful fault experiment is to terminate a NodeManager container while a sufficiently long job is running and observe whether tasks are retried or rescheduled. However, do not assume that killing a worker necessarily causes a particular task to fail: a two-line job may complete before the intervention or may never use that worker. Capture the actual application ID and task attempt information before attributing a change to fault handling. HDFS replication is only one in this laboratory, so terminating a DataNode can also make input blocks unavailable; that is a different failure mechanism from losing a compute-only NodeManager.

For emulator integration experiments, run the same local Hadoop job once with a host input file and once with bytes staged through the emulator's S3 API. Compare SHA-256 digests of the input bytes and the resulting word-count maps. The compute path should be the same after HDFS staging, so a result difference suggests a data-transfer, preprocessing, or execution issue that needs investigation. This design separates storage API fidelity from Hadoop computation correctness.

The module currently does not expose a standard API that maps AWS Emulator `RunJobFlow` or `AddJobFlowSteps` operations to Docker job submission. Implementing such a bridge would require a clearly defined mapping between EMR cluster IDs, Docker Compose projects, instance states, step IDs, HDFS paths, application IDs, logs, and terminal states. It would also need to handle duplicate requests, concurrent submissions, partial failures, and cleanup without claiming successful work that did not execute. That integration is a separate engineering project; this manual documents the actual delivered extension rather than assuming a bridge already exists.

## 24. Suggested Reproducibility Record and Student Evidence

For a reproducible execution, record the source commit of the Scala project, the pinned Apache Hadoop Docker commit, the host architecture, Docker version, JDK version, Scala version, sbt version, and assembly plugin version. Also record the number of configured DataNodes and NodeManagers, the exact input bytes or digest, the compiled JAR digest, the HDFS input/output paths, and the YARN application ID. The expected WordCount map should be stated independently of the observed result, so that a failed assertion cannot be hidden in a narrative description. If AWS Emulator S3 is used, also record the emulator release and the exact output prefix and manifest.

A student demonstration can be organized as a single evidence-driven sequence: show the sbt project structure, compile and assemble the Scala JAR, show the JAR contents, start and inspect the Docker Hadoop cluster, run the built-in smoke test, and then deploy the custom MapReduce job. Next, inspect the YARN application, show reducer part files in HDFS, and compare them with the expected counts. Finally, if the S3 extension is included, show the local emulator identity check, source-object upload, S3-to-HDFS job, and output-object retrieval. Every claimed outcome should have a corresponding observed command result or recorded UI state.

A useful execution log includes:

```text
Experiment identifier:
Date and time:
Host architecture and Docker allocation:
JDK, Scala, sbt, Hadoop, and AWS Emulator versions:
Source commit:
Hadoop Docker commit:
Job JAR path and SHA-256:
Input file path, SHA-256, line count and byte count:
Worker count:
HDFS input and output paths:
YARN application ID and final state:
Number of map and reduce tasks observed:
Expected word-count map:
Actual word-count map:
Application-level assertion result:
Optional emulated S3 input URI:
Optional emulated S3 output prefix and manifest:
Errors, retries, and deviations from the procedure:
```

The record distinguishes implementation evidence from experimental evidence. The source code shows which mapper and reducer are configured; the YARN report shows what was scheduled and executed in a particular run. The assembly archive demonstrates which classes were packaged; the HDFS output demonstrates what the application wrote. These distinctions are important in distributed systems because configurations, intentions, and observed behavior are not interchangeable.

## 25. Quick Reference: Commands in the Required Order

The following is the shortest complete Terminal sequence for a clean first run after installing the extension. All commands are issued from the repository root, and `setup` assumes Docker Desktop is available and the network can fetch the pinned Apache Hadoop sources. The first two phases are independent of AWS Emulator. Execute the optional S3 phase only after the core Hadoop job passes.

Phase A: validate source and dependencies:

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
hadoop-cluster/scripts/preflight.sh
sbt projects
sbt 'hadoopCluster/compile' 'hadoopJob/compile'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli self-test'
```

Phase B: build and start the actual Hadoop daemons:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli setup'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli up 2'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli status'
```

Phase C: prove the Hadoop environment works with the official example:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli smoke'
```

Phase D: package and submit the Scala 3 implementation:

```bash
sbt 'hadoopJob/assembly'
ls -lh hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli wordcount hadoop-cluster/data/wordcount.txt'
```

Phase E: optional AWS Emulator S3 staging:

```bash
source scripts/emulator-env.sh
scripts/check-emulator.sh
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli s3-put hadoop-cluster/data/wordcount.txt cs441-hadoop-staging-001 inputs/wordcount.txt'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli s3-wordcount cs441-hadoop-staging-001 inputs/wordcount.txt results/wordcount'
aws s3 ls s3://cs441-hadoop-staging-001/results/wordcount/ --recursive
```

Phase F: stop the Hadoop cluster after collecting all necessary results:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli down'
```

The included `hadoop-cluster/scripts/quickstart.sh` executes the preflight, setup, up, smoke, assembly, and Scala WordCount stages for the Docker-only path. It does not perform the optional S3 staging phase and does not automatically stop the cluster afterward. For learning and diagnosis, running the commands one at a time is preferable because a failure can be associated with a precise stage. For subsequent stable runs, the quickstart script is convenient provided you understand that setup and image construction may be repeated.

## 26. Frequently Asked Technical Questions

Question: Is the Hadoop cluster actually running on AWS Emulator? No. AWS Emulator runs separately and provides the local AWS API endpoint, while real Hadoop daemons execute in Docker containers on the Mac. The S3 integration transfers bytes between these systems; it does not provision Docker containers through the emulator's EMR APIs. Treating the emulator's EMR metadata as if it were a running YARN cluster would give incorrect execution claims.

Question: Does using two NodeManagers guarantee two concurrent mapper tasks? No. The number and placement of map tasks depend on input splits and scheduling, and a two-line test file may require only one map task. Increasing the worker count changes available capacity, not the application's fundamental work decomposition. Use larger inputs and inspect actual YARN task assignments to study parallelism.

Question: Why are there two reducer files? `WordCountJob` calls `setNumReduceTasks(2)`, instructing the MapReduce framework to create two reducer tasks. Each reducer writes its assigned output partition. The resulting files are distinct partitions, and concatenating their text does not imply a global sort order across partitions.

Question: Why is the Scala 3 runtime included in the JAR but Hadoop libraries are not? Scala's runtime classes are needed by the compiled Scala application and are not assumed to be present in the Hadoop image. The Hadoop API dependencies are declared `Provided`, so they are available for compilation while the Hadoop container supplies them at runtime. This packaging design reduces duplicate Hadoop libraries, but any remaining runtime or classloader incompatibility must still be diagnosed from execution logs.

Question: Can IntelliJ compile `WordCountJob.scala` and then immediately run it using the green gutter icon? It can attempt a direct main-method run, but that does not start the intended YARN application in the Docker virtual cluster. The correct deployment path is to assemble the JAR and submit it through `HadoopClusterCli wordcount`, `run-jar`, or `hadoop jar` inside the NameNode container. The driver expects HDFS and Hadoop runtime configuration and should be evaluated in that environment.

Question: Why does the application work from Terminal but not from IntelliJ? Terminal and IntelliJ can have different working directories, module classpaths, JDKs, PATH variables, AWS environment variables, and VM options. The controller requires the correct `hadoopCluster` classpath and a discoverable module root; S3 execution additionally requires generated emulator credentials and valid TLS properties. Compare the exact first-line Java command and configured process environment before modifying source code.

Question: Can I run arbitrary MapReduce applications using `run-jar`? The current controller accepts a JAR path, a fully qualified main-class name, and exactly two HDFS paths. Applications requiring a different argument protocol need an adapted controller or direct Hadoop CLI submission. The controller also reads text `part-*` outputs, so binary outputs and unusual destination layouts require different result retrieval code.

Question: Why does Hadoop refuse a second run with the same output path? Hadoop's output format normally requires that the destination directory not already exist. Refusing to overwrite prevents accidental loss of a previous result and is unrelated to AWS Emulator. Use the convenience commands with generated paths or explicitly select a new output directory in a manual job submission.

Question: What does a successful result demonstrate? It demonstrates that a particular compiled archive, Hadoop runtime, input, and cluster configuration produced the expected output in that run. It does not establish cloud availability, production fault tolerance, or compatibility with every AWS EMR feature. For an experimental claim, identify the tested environment and retain the supporting job logs and artifacts.

Question: Can this be extended to Scala-based Hadoop research? Yes. Replace or augment the `hadoopJob` source with the desired mapper, reducer, partitioner, input format, or driver, and continue using the controller to deploy an assembled JAR if the application's arguments fit the existing interface. More complex applications may need new task parameters, custom job configurations, or a new result retrieval method. The overall separation between local execution and emulated AWS API testing remains useful.

## 27. Validation Limits and References

This manual was produced by inspecting the actual corrected extension files: `hadoop-cluster.sbt`, `project/99-cs441-hadoop-assembly.sbt`, `HadoopClusterCli.scala`, `HadoopDocker.scala`, `CommandRunner.scala`, `EmulatorS3.scala`, `WordCountJob.scala`, the shell scripts, and the Docker Compose platform override. The documented project IDs, class names, task names, build artifact path, main-class invocation, HDFS path restrictions, and S3 result-prefix behavior are grounded in that code. The guidance also reflects the earlier local troubleshooting sequence for IntelliJ endpoint configuration, malformed TLS trust store options, and the original DynamoDB table rerun conflict. Those preceding failures are distinguished from Hadoop application failures rather than merged into a generic error category.

The source code and commands have been reviewed statically, but the complete build and Docker execution sequence has not been run by the authoring environment. Therefore, every output shown as illustrative should be validated on the target Mac, especially architecture-sensitive image startup, Scala 3/Hadoop class compatibility, YARN resource scheduling, and S3 API behavior for the installed AWS Emulator release. If a command fails, preserve the exact error and the first causal stack-trace message before changing any configuration. A successful final demonstration is one in which the real YARN application completes and the actual reducer outputs match the independently defined expected counts.

Authoritative background resources:

1. Apache Hadoop 3.4.2 Docker configuration: https://github.com/apache/hadoop/tree/docker-hadoop-3.4.2
2. Apache Hadoop 3.4.2 MapReduce Tutorial: https://hadoop.apache.org/docs/r3.4.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
3. Apache Hadoop YARN documentation: https://hadoop.apache.org/docs/r3.4.2/hadoop-yarn/hadoop-yarn-site/YARN.html
4. Apache Hadoop HDFS user guide: https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html
5. Official AWS Emulator source: https://github.com/aws/aws-emulator
6. AWS Emulator documentation: https://docs.aws.amazon.com/emulator/latest/userguide/what-is-service.html
7. sbt-assembly plugin documentation: https://github.com/sbt/sbt-assembly
8. IntelliJ IDEA sbt support: https://www.jetbrains.com/help/idea/sbt-support.html
9. IntelliJ IDEA sbt Task configurations: https://www.jetbrains.com/help/idea/run-debug-configuration-sbt-task.html
10. IntelliJ IDEA Scala execution: https://www.jetbrains.com/help/idea/run-debug-and-test-scala.html

In summary, this extension implements a real Hadoop MapReduce teaching environment with a separate, optional emulator-backed S3 integration. The assembly JAR is produced by `hadoopJob/assembly`, deployed into the NameNode container by the controller or Docker CLI, executed through `hadoop jar` using YARN, and verified by inspecting HDFS reducer results. IntelliJ IDEA supports the workflow either through sbt Task configurations or through a correctly configured direct Application launcher for the host-side controller. The resulting architecture makes it possible to study distributed execution without pretending that an emulated AWS EMR control plane is itself a functioning Hadoop cluster.
