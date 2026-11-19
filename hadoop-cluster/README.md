> Installation note (corrected patch): place `hadoop-cluster.sbt` in the repository
> root and `project/99-cs441-hadoop-assembly.sbt` in `project/`. These are added
> automatically by the corrected patch and do not replace your existing `build.sbt`,
> `.gitignore`, `project/build.properties`, or `project/plugins.sbt`.
> See `HADOOP_EXTENSION_INSTALL.md` for exact commands.

# Real Hadoop in Docker, AWS Emulator on the side

This directory is a separate Scala 3 module added to the `aws-emulator-scala3-lab` project. It supplies real local Hadoop MapReduce execution through Apache Hadoop 3.4.2 containers and an optional data-staging pipeline to the local AWS Emulator S3 API. Its scope is deliberately explicit: AWS Emulator emulates some Amazon EMR control-plane behavior, but an emulated EMR cluster record is not a general-purpose Hadoop YARN cluster. The Hadoop work below actually runs within separate Linux containers and uses a live HDFS NameNode, DataNodes, YARN ResourceManager, and NodeManagers.

The module has two sbt projects that are intentionally not aggregated into the original root project, so existing `sbt compile` and `sbt runMain AwsEmulatorLab` commands retain their original scope. `hadoopCluster` provides the Scala 3 controller and the local AWS S3 adapter; `hadoopJob` provides an independently compiled Scala 3 MapReduce WordCount job. The original `AwsEmulatorLab.scala` and its fixed-resource-name behavior remain unchanged, so its documented DynamoDB rerun error is independent of this new module.

## 1. Project structure

```text
hadoop-cluster/
  README.md
  data/wordcount.txt                         fixed reproducible input
  docker-compose.platform.yaml              explicit AMD64 image platform
  scripts/preflight.sh                       prerequisite checks
  scripts/quickstart.sh                      end-to-end local Hadoop path
  src/main/scala/edu/cs441/hadoop/cluster/
    CommandRunner.scala                      direct ProcessBuilder runner
    HadoopDocker.scala                       Docker + HDFS + YARN orchestration
    EmulatorS3.scala                         fail-closed AWS Emulator S3 client
    HadoopClusterCli.scala                   command-line entry point
  jobs/wordcount/src/main/scala/edu/cs441/hadoop/jobs/
    WordCountJob.scala                       Mapper, combiner, reducer and driver
  .runtime/apache-hadoop-docker/             created by setup; ignored by Git
```

`CommandRunner` never invokes a host shell to interpolate user-supplied strings; it passes a sequence of arguments directly to `ProcessBuilder`. It gives each child process a deadline, drains its combined output while the child runs, bounds captured log data to 2 MiB, reports nonzero exit codes, and stops an operation that exceeds its deadline. These choices keep a failed external command from being silently treated as a successful Hadoop job, although they do not turn arbitrary third-party JARs into safe software. Only run trusted Hadoop applications on this development machine.

`HadoopDocker` manages Apache's published `docker-hadoop-3.4.2` branch, pinned to commit `0f4c272c0151e3e897b443d385aef6049603dbd4`. The branch contains a Dockerfile, Compose specification, and Hadoop configuration. The local `docker-compose.platform.yaml` is merged with Apache's Compose file to select the published AMD64 image consistently, including on Apple Silicon hosts. The `setup` command clones the configuration, creates the missing `test.sh` file required by the Compose mount, and builds the Hadoop image using Apache's Hadoop 3.4.2 archive. The implementation creates two DataNodes and two NodeManagers by default, and it uses a stable Compose project name, `cs441-hadoop`, rather than the AWS Emulator container name.

`EmulatorS3` accesses only `https://127.0.0.1:9297`, checks for the emulator's generated credentials and PKCS12 trust store, and verifies that STS returns account `000000000000` before accessing S3. It creates an example bucket only when it does not already exist, downloads input to a temporary host file, and uploads Hadoop part files plus a manifest to a unique key prefix. It does not call Amazon EMR, and its successful STS call is not a substitute for testing actual IAM authorization in a real AWS environment.

## 2. Prerequisites and resource requirements

On macOS, install Docker Desktop, JDK 21, sbt (recommended sbt 1.10.x), and Git. The Apache `hadoop-runner` image used by the pinned Dockerfile is published as `linux/amd64`; on Apple Silicon the included Compose override explicitly selects that platform. This requires Docker Desktop's AMD64 emulation to be available, may run appreciably slower than a native ARM64 image, and must not be mistaken for a native Apple Silicon Hadoop build. The S3 examples additionally require the AWS Emulator process, its generated files under `$HOME/.aws/emulator/9297`, and AWS CLI v2 for the preflight identity check. Hadoop images and dependency caches consume significant disk space; reserve roughly 15 GB or more for images and downloads and allocate approximately 8 GB of memory to Docker Desktop when possible. Two NodeManagers need enough headroom to start YARN containers; on a smaller Mac, use `up 1` instead of the default `up 2`.

From the *repository root*, execute:

```bash
hadoop-cluster/scripts/preflight.sh
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli self-test'
```

The `self-test` command checks local key validation and tokenization. It does not connect to Docker, AWS, or a running Hadoop service, and it therefore cannot establish that a MapReduce cluster is available. The separate `preflight.sh` checks the presence of Git, Docker, Java, and sbt and verifies that Docker Desktop is responding.

## 3. Build and start Hadoop

Execute the following commands in a terminal opened at the top-level `aws-emulator-scala3-lab` directory:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli setup'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli up'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli status'
```

`setup` first obtains the official Apache Docker configuration. Its image build fetches the Hadoop release tarball, so an internet connection is needed for the first build, and a build failure may indicate a proxy, mirror, network, or Docker Desktop issue rather than a Scala issue. `up` starts the containers and repeatedly checks that the requested number of HDFS DataNodes and YARN NodeManagers are observable; unlike merely checking `docker compose ps`, this checks services' reported readiness. In a successful default startup, the console reports two live DataNodes and two active NodeManagers.

The Hadoop monitoring interfaces should then be reachable at `http://localhost:9870` for the HDFS NameNode and `http://localhost:8088` for YARN's ResourceManager. These ports are published by Apache's Compose example; they are separate from the AWS Emulator HTTPS port 9297. The default Apache example is a local teaching configuration, not a hardened publicly exposed service. Keep it restricted to a development machine and avoid running sensitive production datasets within it.

If the Mac is resource constrained, execute:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli up 1'
```

The code checks for the number of DataNodes and NodeManagers requested. The Apache configuration uses `dfs.replication=1` by default, so even with two DataNodes a file block normally has one replica, not two. To study true HDFS block replication behavior, modify a copy of the Hadoop configuration, set replication to two or more, and run a separate experiment; doing so is distinct from scaling the number of containers.

## 4. First independent check: execute Apache's built-in MapReduce job

The first test deliberately requires neither the Scala 3 WordCount assembly nor an AWS Emulator process. It executes Apache's own `hadoop-mapreduce-examples-3.4.2.jar` inside the actual Hadoop cluster. This isolates Hadoop installation, YARN resource scheduling, HDFS data movement, and network configuration from Scala job packaging.

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli smoke'
```

For its input file `data/wordcount.txt`:

```text
cloud hadoop cloud
hadoop scala cloud
```

The expected word frequencies are `cloud 3`, `hadoop 2`, and `scala 1`. The controller stages the text file into a unique HDFS path under `/user/hadoop/cs441/input`, runs the built-in example JAR, and concatenates the `part-*` output files. It parses the results and compares the actual key-count pairs with the expected map; it does not assume that keys are returned in any specific line order. The assertion is performed only after the Hadoop subprocess reports success.

The YARN web interface shows the submitted application and task attempts. If a mapper fails before the reducers start, its YARN logs and application state should be inspected rather than assuming that a successful HDFS file upload means the job executed. The Hadoop MapReduce sequence consists of reading input splits, mapping each line to emitted key-value pairs, partitioning by key, shuffling intermediate values, aggregating each key, and writing part files in HDFS.

## 5. Compile the Scala 3 mapper and reducer

Build the example JAR from the repository root:

```bash
sbt 'hadoopJob/assembly'
```

The sbt subproject declares Hadoop 3.4.2 compile-time dependencies in the `provided` scope because Hadoop's runtime already supplies its own core libraries. The `sbt-assembly` plugin packs the Scala 3 application classes and their non-provided runtime dependencies, including the Scala runtime, into `hadoop-cluster/jobs/wordcount/target/scala-3.9.0/cs441-wordcount.jar`. The assembly deliberately omits a Main-Class manifest entry because `hadoop jar` is passed the fully qualified entry-point name explicitly; combining an entry-point manifest with an extra class argument would shift the application arguments. This matters because simply sending a thin Scala application JAR to a Hadoop cluster can fail with class-loading errors when Scala runtime libraries are missing.

The `WordCountMapper` receives a Hadoop `LongWritable` byte-offset key and a `Text` input line. It lowercases the input using a locale-independent mapping, separates sequences of Unicode letters and digits into words, and emits a `(word, 1)` pair for each token. The `WordCountReducer` receives each word and a Java iterable of integer counts, sums them with a 64-bit intermediate accumulator, checks for overflow, and emits one `(word, total)` result. The same reducer is registered as a combiner because integer addition is associative and commutative, so local pre-aggregation does not change the intended sum.

For example, the first line emits `(cloud,1)`, `(hadoop,1)`, and `(cloud,1)`. A combiner may transform this local stream into `(cloud,2)` and `(hadoop,1)`. The second line emits `(hadoop,1)`, `(scala,1)`, and `(cloud,1)`; during shuffle, all `cloud` contributions are routed to the same reducer partition, giving `(cloud,3)` after aggregation. The job requests two reducer tasks, so the output may contain multiple `part-r-*` files; the controller treats their collective contents as one logical result.

Execute the custom job:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli wordcount hadoop-cluster/data/wordcount.txt'
```

This command copies the input into a new HDFS directory, copies the assembled JAR into the NameNode container, invokes `hadoop jar` with the fully qualified Scala entry point, and prints the resulting word counts. It refuses to silently overwrite existing HDFS result directories. Because each ordinary `wordcount` command generates a unique output path, rerunning the job does not recreate the previous project's DynamoDB duplicate-table error.

## 6. Run an existing Hadoop MapReduce JAR

The entry point accepts a separately built Hadoop JAR when it is compatible with the Hadoop 3.4.2 runtime. This permits migrating an existing Java or Scala MapReduce application to the same local cluster without modifying AWS Emulator. The controller restricts HDFS staging and output paths to `/user/hadoop/cs441/` so an accidental argument cannot delete or replace an unrelated HDFS directory.

First, stage an input file in HDFS:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli stage hadoop-cluster/data/wordcount.txt /user/hadoop/cs441/input/my-job/input.txt'
```

Then run the previously compiled JAR, substituting its path and main class:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli run-jar /absolute/path/to/my-mapreduce.jar com.example.MyJob /user/hadoop/cs441/input/my-job/input.txt /user/hadoop/cs441/output/my-job-001'
```

Inspect the output directory:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli show /user/hadoop/cs441/output/my-job-001'
```

A custom JAR may need external third-party dependencies, additional Hadoop properties, non-text input formats, or multiple command-line arguments. The provided `run-jar` interface intentionally implements the common two-path argument convention; it is not an arbitrary EMR step interpreter and does not implement arbitrary Hadoop job command formats. For more complicated applications, extend the argument vector in `HadoopDocker.runJar`, preserving direct subprocess invocation and explicit resource constraints.

## 7. Use AWS Emulator S3 to stage and persist Hadoop data

AWS Emulator can store the input and output while the actual computation occurs inside Hadoop. The supplied pipeline uses explicit `S3 -> host temporary file -> docker cp -> HDFS` staging, followed by `HDFS -> docker cp -> host temporary results -> S3`. This avoids assuming that Hadoop's S3A filesystem connector automatically trusts the emulator's private certificate or can resolve its localhost endpoint from within a container. Direct Hadoop S3A connectivity is a possible follow-on extension, but requires Docker host networking, AWS SDK compatibility, credentials, path-style addressing, and a correctly trusted certificate chain.

Start AWS Emulator using the procedure in the top-level README. In the same terminal from which you will launch sbt, load its generated environment and verify the identity:

```bash
source scripts/emulator-env.sh
scripts/check-emulator.sh
```

The check must print account `000000000000`. The environment includes `AWS_EMULATOR_JAVA_TRUSTSTORE`, which `hadoop-cluster.sbt` passes into the forked JVM when running `hadoopCluster`; the build JVM retains its usual default trust settings for Maven. The S3 adapter rejects a nonlocal `AWS_ENDPOINT_URL`, unexpected credential file paths, a missing or incorrectly configured PKCS12 trust store, an unexpected Region, and explicit ambient real-AWS credential variables.

Create a test S3 input in the local emulator:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli s3-put hadoop-cluster/data/wordcount.txt cs441-hadoop-staging-001 inputs/wordcount.txt'
```

Build the Scala job if this has not already been done:

```bash
sbt 'hadoopJob/assembly'
```

Execute the S3-to-Hadoop-to-S3 integration:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli s3-wordcount cs441-hadoop-staging-001 inputs/wordcount.txt results/wordcount'
```

A successful run prints the same logical word counts as the first smoke test, then uploads the reducer output into a unique S3 prefix under `results/wordcount/job-...`. It also uploads `manifest.json`, recording that the computation ran on local Docker Hadoop rather than AWS Emulator's EMR cluster model. The manifest records the HDFS output path and the number of reducer part files. Because the directory uses a run-specific suffix, one run does not intentionally overwrite the output of an earlier successful run.

Use the AWS CLI to examine the uploaded objects:

```bash
aws s3 ls s3://cs441-hadoop-staging-001/results/wordcount/ --recursive
```

The program deletes temporary host-side staging files after the pipeline finishes. It does not delete the HDFS input and output directories, the emulator S3 objects, or the Docker containers; those remain inspectable for classroom experiments. For sensitive information, introduce an explicit retention policy and secure cleanup. The emulator is not approved for testing production security controls or storing sensitive operational data.

## 8. IntelliJ IDEA execution

Import the top-level `build.sbt` rather than importing `hadoop-cluster` as an unrelated project. IntelliJ should discover the root project and the `hadoopCluster` and `hadoopJob` sbt subprojects. For the controller, create an Application run configuration with main class `edu.cs441.hadoop.cluster.HadoopClusterCli`, working directory set to the repository root, and program arguments such as `smoke`, `status`, or `wordcount hadoop-cluster/data/wordcount.txt`. Use the `hadoopCluster` module's classpath for this configuration.

For `smoke`, no AWS environment variables are required because the command uses Docker and Hadoop only. For `s3-put` and `s3-wordcount`, pass the exact variables from `scripts/emulator-env.sh` into IntelliJ's Environment Variables field. Since IntelliJ launches Java directly, enter the emulator TLS properties in VM Options as two separate JVM arguments, not concatenated with any other path:

```text
-Djavax.net.ssl.trustStore=/Users/drmark/.aws/emulator/9297/truststore.p12
-Djavax.net.ssl.trustStoreType=PKCS12
```

A malformed property such as `-Djavax.net.ssl.trustStoreType=PKCS12/Users/...` produces `No X509TrustManager implementation available`; this is an IntelliJ launch-configuration problem, not evidence of a broken AWS SDK. A missing `AWS_ENDPOINT_URL` results in an immediate configuration refusal before any network access. The adapter checks these conditions deliberately, reproducing the safety lessons from the original AWS Emulator lab.

## 9. Diagnosing problems

| Symptom | Likely cause | Recommended action |
|---|---|---|
| `Cannot locate hadoop-cluster` | Wrong working directory | Run sbt at the repository root or set `HADOOP_CLUSTER_ROOT` |
| `Hadoop Docker directory absent` | Setup was not run | Execute the `setup` command |
| `docker compose build` fails | Docker network, Apache mirror, or image build problem | Verify Docker Desktop, network access, and inspect upstream Dockerfile output |
| `Hadoop not ready` | NameNode/YARN still starting, or insufficient Docker memory | Run `status`, inspect container logs, allocate memory or use `up 1` |
| `HDFS input does not exist` | Wrong HDFS input path | Stage with the `stage` command and verify it exists |
| `HDFS output already exists` | Hadoop does not overwrite an output directory | Choose a new output path; do not blindly delete data |
| WordCount job fails in YARN | Java/Scala JAR packaging, resource or task failure | Inspect the YARN UI and NodeManager logs |
| `Build it first with hadoopJob/assembly` | Custom Scala 3 JAR missing | Run the assembly command |
| `No X509TrustManager implementation available` | Broken JVM PKCS12 options | Set both TLS JVM properties correctly |
| `AWS_ENDPOINT_URL` rejected | Nonlocal or missing endpoint | Source `scripts/emulator-env.sh` in the invoking shell |
| Unexpected account from STS | Possibly wrong identity context | Stop; verify the emulator endpoint and credentials; never switch to real AWS |
| S3 404 | Missing source bucket or key | Run `s3-put`, then check the local S3 listing |
| Port 9870 or 8088 already in use | Another local Hadoop instance | Stop conflicting service or change Apache's Compose mapping |

When a container does not start, inspect the actual Docker logs using the upstream checkout created by `setup`:

```bash
cd hadoop-cluster/.runtime/apache-hadoop-docker
docker compose --project-name cs441-hadoop -f docker-compose.yaml -f ../../docker-compose.platform.yaml ps
docker compose --project-name cs441-hadoop -f docker-compose.yaml -f ../../docker-compose.platform.yaml logs --tail 100 namenode datanode resourcemanager nodemanager
```

The `CommandRunner` captures at most 2 MiB of a child process's combined log output, and errors include exit status and captured text. More detailed YARN task logs are available from the ResourceManager interface and through the normal Hadoop CLI inside the containers. A failed `hadoop jar` command is not silently converted into a successful EMR-style status.

## 10. Cleanup, limitations, and future work

To stop the Hadoop containers from the repository root:

```bash
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli down'
```

In this Apache Compose setup, HDFS files may live on the containers' ephemeral filesystems and can be lost when containers are removed. The `down` command deliberately does not delete anything in AWS Emulator S3, and it does not attempt to delete arbitrary Docker volumes by default. Treat the HDFS dataset as disposable, retain any important results in the emulated S3 store, and remember that the emulated S3 data also disappears when the emulator process is reset.

This local cluster consists of separate container processes sharing one physical Mac, not independently failing hardware hosts. Its YARN scheduling and HDFS behavior are genuine, but its topology is not representative of a multi-rack production deployment, and its memory, disk I/O, and network throughput are constrained by Docker Desktop. Consequently, its results are appropriate for functional and introductory scheduling experiments, not for quantitative claims about AWS EMR performance, cost, cloud availability, or failure independence.

The present module does not create a transparent bridge from `RunJobFlow` or `AddJobFlowSteps` in the emulator to Docker containers. Implementing such a bridge would be a separate project that must define cluster-identity mapping, resource provisioning, step status propagation, event ordering, logs, cleanup, security, and correctness of error states. This module instead presents a dependable separation: emulated APIs for cloud-facing compatibility tests and actual Hadoop daemons for MapReduce execution.

The strongest next steps are a persistable HDFS volume configuration, unit-tested job packaging, metrics capture from YARN, fault-injection experiments that remove a NodeManager during a job, and a test harness that compares the same MapReduce input across local and a carefully budgeted real EMR cluster. Reproducibility requires recording the Hadoop Docker commit, JVM version, Scala version, emulator release, input bytes, worker count, and job JAR hash. Never compare a simulated EMR control-plane response with a real MapReduce task execution as though they were equivalent measurements.

### References

- [AWS Emulator repository](https://github.com/aws/aws-emulator)
- [Apache Hadoop Docker branch, version 3.4.2](https://github.com/apache/hadoop/tree/docker-hadoop-3.4.2)
- [Apache Hadoop runner image platforms](https://hub.docker.com/r/apache/hadoop-runner/tags)
- [Apache Hadoop MapReduce documentation](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- [Apache Hadoop YARN documentation](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)
- [sbt-assembly plugin](https://github.com/sbt/sbt-assembly)

The scripts and source have been structurally checked, but they have not been executed against Docker or compiled by sbt in the artifact-authoring environment. Validate `self-test`, then `smoke`, and only afterward the compiled Scala JAR and S3 integration on the target Mac. This statement distinguishes planned expected behavior from results observed in an actual run.
