# AWS in a Bottle: The Cloud You Can Break Before You Ship

## The AWS Emulator and Scala 3 companion laboratory

Build it, break it, blame the emulator: all the fun of AWS without the surprise credit-card bill.

This repository is a runnable companion to the CS441 Pressbooks chapter, *AWS in a Bottle: The Cloud You Can Break Before You Ship*. It illustrates how a Scala 3 application talks to local implementations of AWS APIs through the AWS SDK for Java 2.x. It also demonstrates why a successful API call is not necessarily a correct distributed application, why repeated executions require explicit state management, and why evidence from an emulator cannot replace security validation and selected tests against actual AWS services.

This README documents the distributed-systems concepts, project architecture, file-by-file implementation, prerequisites, macOS and IntelliJ IDEA configuration, command-line workflow, expected observations, experimental limitations, cleanup, troubleshooting, and three actual failures encountered while running this laboratory on macOS. It is intentionally self-contained: following the main path requires only this repository, a running AWS Emulator, Docker Desktop, the AWS CLI, a JDK, and sbt. The document also explains how to adapt the experiments for Linux, Windows, continuous integration (CI), and more ambitious research projects.

Document revised November 18, 2026. This companion project specifies Scala **3.9.0** and AWS SDK for Java **2.55.12** in `build.sbt`; use those versions when reproducing the original laboratory. The commands and SDK use have been checked against the source in this archive and the linked AWS documentation. Earlier end-to-end behavior was not independently executed in the document-authoring environment; however, a real IntelliJ session reported successful identity verification and S3 round-trip behavior, followed by the documented DynamoDB existing-table error. Do not interpret that partial run as evidence that every experiment or every AWS Emulator release passes.

## Contents

For a first run, follow Sections 4–7. For IntelliJ IDEA, use Section 8. For errors observed in actual executions, consult Section 11; for repeatable runs and cleanup, consult Section 12.

- [1. Executive overview and acceptance criteria](#1-executive-overview-and-acceptance-criteria)
- [2. Concepts and terminology](#2-concepts-and-terminology)
- [3. Project file inventory](#3-project-file-inventory)
- [4. Required software and preflight checks](#4-required-software-and-preflight-checks)
- [5. Start AWS Emulator with Docker](#5-start-aws-emulator-with-docker)
- [6. Verify a local-only CLI session](#6-verify-a-local-only-cli-session)
- [7. Run all four Scala programs from Terminal](#7-run-all-four-scala-programs-from-terminal)
- [8. Configure IntelliJ IDEA on macOS](#8-configure-intellij-idea-on-macos)
- [9. Source-level walkthrough of AwsEmulatorLab.scala](#9-source-level-walkthrough-of-awsemulatorlabscala)
- [10. A concrete execution trace and the distributed transaction boundary](#10-a-concrete-execution-trace-and-the-distributed-transaction-boundary)
- [11. The three observed runtime failures and their resolutions](#11-the-three-observed-runtime-failures-and-their-resolutions)
- [12. Safe cleanup and reproducible reruns](#12-safe-cleanup-and-reproducible-reruns)
- [13. TestSelector.scala: choosing experiments under a time budget](#13-testselectorscala-choosing-experiments-under-a-time-budget)
- [14. RetryPlan.scala: classify errors before retrying](#14-retryplanscala-classify-errors-before-retrying)
- [15. SqsVisibilityProbe.scala: inspect visibility timeout behavior](#15-sqsvisibilityprobescala-inspect-visibility-timeout-behavior)
- [16. How to interpret fidelity and emulator limitations](#16-how-to-interpret-fidelity-and-emulator-limitations)
- [17. Security boundary, costs, and safe engineering practice](#17-security-boundary-costs-and-safe-engineering-practice)
- [18. Advanced extension: AWS agents, Smithy, and evidence-gated development](#18-advanced-extension-aws-agents-smithy-and-evidence-gated-development)
- [19. Reproducible classroom or CI procedure](#19-reproducible-classroom-or-ci-procedure)
- [20. Current status and what remains to be improved](#20-current-status-and-what-remains-to-be-improved)
- [21. Frequently asked questions](#21-frequently-asked-questions)
- [22. Source documents and further reading](#22-source-documents-and-further-reading)

## 1. Executive overview and acceptance criteria

The laboratory's main executable is the Scala object `AwsEmulatorLab`. It connects to an AWS Emulator process listening only at `https://127.0.0.1:9297`, proves that the reported emulated account is `000000000000`, and exercises three AWS service interfaces: Amazon S3, Amazon DynamoDB, and Amazon Simple Queue Service (SQS). The process uses the AWS SDK for Java from Scala 3; the SDK sends ordinary AWS-style requests to the explicitly configured local endpoint rather than to the public AWS network.

A clean, successful run demonstrates the following sequence:

1. Contact AWS Security Token Service (STS), obtain the emulated account identifier, and abort if it is not `000000000000`.
2. Create an S3 bucket named `cs441-emulator-lab-001`; write the JSON object `papers/p-001.json`; read the object back; and compare its bytes with the original payload.
3. Create a DynamoDB table named `CS441Ingest` with string partition key `id`; wait until the table exists; insert an item representing paper `p-001`; and read it back with strongly consistent read semantics.
4. Attempt the same DynamoDB conditional insertion again and verify that DynamoDB rejects it with `ConditionalCheckFailedException`.
5. Create an SQS queue named `cs441-indexing-events`; send an indexing instruction; receive a message; check its body; and acknowledge receipt by deleting the message using its receipt handle.
6. Print the final success marker only after all these operations finish without an unexpected exception.

The exact final marker is:

```text
LAB PASS: S3, DynamoDB, and SQS round trips completed
```

A successful run establishes an important but bounded claim: the selected operations behaved as expected in the installed emulator for that execution. It does **not** establish that the three operations form a transaction, that the emulator implements every feature of the real AWS services, that IAM permissions are correct, or that the application is suitable for production. In particular, S3, DynamoDB, and SQS each maintain their own state, and the sample program does not implement distributed rollback, an outbox, automatic recovery, or a durable orchestration layer.

## 2. Concepts and terminology

### 2.1 What the emulator does

AWS Emulator is a program that implements selected AWS API operations locally. An API, or application programming interface, specifies how a client expresses an operation and interprets its response. For example, an S3 `PutObject` request names a bucket and object key, whereas a DynamoDB `PutItem` request supplies a table name, item attributes, and optional conditions. The emulator receives these requests over HTTPS, routes them to its service implementations, modifies local emulated state, and returns responses in AWS-compatible formats to the extent supported by that release.

An SDK, or software development kit, exposes these APIs as programming-language methods. In this project, the methods belong to the AWS SDK for Java 2.x, but the source is written in Scala 3 because Scala runs on the Java Virtual Machine (JVM) and interoperates with Java classes. The SDK constructs requests, signs them, sends them over an HTTP client, deserializes responses, and maps AWS-style error responses into typed exceptions. Consequently, the laboratory can use realistic client-side behavior without creating chargeable AWS resources.

The endpoint is the network address to which the SDK connects. The required endpoint here is the HTTPS URL `https://127.0.0.1:9297`, where `127.0.0.1` is the IPv4 loopback address and `9297` is the emulator port. The application explicitly rejects any other endpoint; its hard-coded restriction is an important protection against accidental use of real AWS. The chosen AWS Region is `us-east-1`, and the example uses that Region consistently across all four clients.

### 2.2 Control plane, data plane, and state

A service's **control plane** manages resources and configuration: for example, `CreateBucket`, `CreateTable`, or `CreateQueue`. Its **data plane** performs the service's operational work: for example, `PutObject` and `GetObject`, `PutItem` and `GetItem`, or `SendMessage` and `ReceiveMessage`. The distinction matters because success creating a resource does not prove that storing, retrieving, ordering, retrying, or delivering data matches actual AWS behavior. AWS documents that emulation fidelity currently varies by service and is generally stronger for control-plane behavior than for some data-plane features.

The **state** of a service is the information it retains between calls. In this laboratory, the S3 state includes a bucket and an object, the DynamoDB state includes a table and an item, and the SQS state includes a queue and messages that may be visible, temporarily invisible, or deleted. The emulator maintains its emulated resources in memory for the lifetime of the running process. Restarting it starts a fresh resource universe, whereas executing the Scala application twice without restarting leaves the previous resources available to the second run.

### 2.3 Identity, signatures, credentials, and certificates

STS, short for Security Token Service, provides the `GetCallerIdentity` API used in the application's first substantive check. AWS Emulator creates a local identity under the emulated account `000000000000`; that value is the guard used throughout this lab. The generated `emulator_credentials` file contains temporary credentials used to sign AWS-style API calls to this particular emulator process. These credentials are not real AWS account credentials, and they are regenerated when the emulator starts again.

TLS, or Transport Layer Security, secures the HTTPS connection between the SDK and the emulator. Because a local emulator does not normally have a certificate signed by the operating system's default trusted public certificate authorities, it generates its own certificate authority file `ca.pem`. The AWS CLI can use `AWS_CA_BUNDLE` to trust that certificate, while this JVM application uses a generated PKCS12 trust store named `truststore.p12`. PKCS12 is a format for storing certificates and, in other contexts, private keys; in this case, Java uses it as a collection of trust anchors for validating the server certificate.

These are separate checks. An SDK can know the correct local URL yet reject its certificate before an AWS API request is processed. Similarly, a correct certificate does not imply valid credentials, and valid credentials do not guarantee that a requested operation is supported. Distinguishing endpoint selection, TLS validation, request signing, and service behavior prevents random, potentially unsafe configuration changes during debugging.

### 2.4 Idempotency, conditional operations, and at-least-once delivery

An operation is **idempotent** if repeating it leaves the externally relevant result in the same intended state as performing it once. The statement means neither that the operation is free nor that every repetition returns the same HTTP status. For example, a table-creation request may correctly reject a second attempt because the table already exists, even though the desired infrastructure state, “the table exists,” is already satisfied. Infrastructure setup can be made idempotent by checking existing resources and verifying their configuration before creating anything absent.

A DynamoDB conditional write is different: it deliberately requires a precondition before changing state. Here `attribute_not_exists(id)` means an item with the specified primary key must not already be present. The first insert of `p-001` succeeds, and the second insert of `p-001` is expected to fail with `ConditionalCheckFailedException`. The duplicate rejection demonstrates a useful uniqueness property, but it also means that ignoring an existing-table error alone cannot make the entire lab rerunnable.

SQS typically supports at-least-once delivery: processing logic must be prepared to receive a logically identical message more than once. A receipt handle identifies one receive operation and is used for message deletion, whereas the message body describes the application-level work. A visibility timeout temporarily hides a received message from other consumers, but receipt and processing are not a distributed transaction. Production consumers therefore need idempotent processing, appropriately designed retries, and durable failure recovery.

## 3. Project file inventory

The project is intentionally small; each file demonstrates a distinct part of the cloud testing workflow. It is not a fork of `aws/aws-emulator` and does not contain the emulator's Rust implementation. It is a Scala consumer of the emulator's API endpoints.

```text
aws-emulator-scala3-lab/
├── README.md                              This guide
├── build.sbt                              Scala version, dependencies, forked JVM
├── Dockerfile.arm64                       Apple Silicon / ARM64 emulator package
├── Dockerfile.amd64                       Intel or AMD x86_64 emulator package
├── scripts/
│   ├── emulator-env.sh                    Local-only shell environment
│   └── check-emulator.sh                  TLS health and STS identity gate
└── src/main/scala/
    ├── AwsEmulatorLab.scala               Main STS + S3 + DynamoDB + SQS example
    ├── TestSelector.scala                 Exhaustive test-budget selection
    ├── RetryPlan.scala                    Illustrative bounded retry decision
    └── SqsVisibilityProbe.scala           Optional queue timing observation
```

`build.sbt` pins the language version to `3.9.0`, selects organization `edu.cs441` and project version `0.1.0`, and declares four AWS SDK dependencies: `sts`, `s3`, `dynamodb`, and `sqs`, all at `2.55.12`. It also configures `Compile / run / fork := true`, meaning that sbt's `run` task launches a separate application JVM. When the environment includes `AWS_EMULATOR_JAVA_TRUSTSTORE`, sbt passes its path to the child JVM using the JVM properties `javax.net.ssl.trustStore` and `javax.net.ssl.trustStoreType=PKCS12`.

`Dockerfile.arm64` and `Dockerfile.amd64` install an AWS-provided emulator RPM into an Amazon Linux container and expose port `9297`. Select the Dockerfile matching the machine's CPU architecture. Both images run the emulator inside the container with a listener bound to `0.0.0.0` internally; the *Docker port publishing rule* must still be restricted to `127.0.0.1` on the host to prevent access from other machines.

`emulator-env.sh` clears the common environment variables that might carry real AWS credentials, and sets the local endpoint, credential paths, certificate paths, Region, and metadata settings. `check-emulator.sh` first requires that the endpoint equals `https://127.0.0.1:9297`, then performs an HTTPS health check with the generated certificate authority and confirms the emulated STS account. These scripts are intended to run in a Unix-style shell and must not be assumed to configure an independently launched IntelliJ process.

`AwsEmulatorLab.scala` is the stateful integration example. `TestSelector.scala` and `RetryPlan.scala` perform local calculations and do not call AWS Emulator at all. `SqsVisibilityProbe.scala` does call the emulator and deliberately makes timing observations whose interpretation depends on the installed emulator release. Running the four programs separately helps distinguish a problem with Scala compilation from a problem with networking, TLS, resource state, or service semantics.

## 4. Required software and preflight checks

The documented macOS development setup uses Docker Desktop, AWS CLI version 2, OpenJDK 21, sbt, and optionally IntelliJ IDEA. Docker runs the Linux emulator package in a container because the repository's documented source-build path is Linux; it does not require compiling the enormous Rust emulator workspace on macOS. The AWS CLI is used for preflight checks, state inspection, and cleanup, while sbt compiles the Scala 3 code and manages the Java libraries through its dependency resolver.

Run the following commands from Terminal before attempting the laboratory:

```bash
docker --version
docker info
aws --version
java -version
sbt --version
```

`docker info` should succeed while Docker Desktop is running. The Java runtime should be compatible with the installed Scala and SDK versions; the actual macOS test described in this README used OpenJDK 21. Version numbers in `build.sbt` are explicit and should not be silently replaced by “latest” when investigating a regression. An sbt launch or Java compilation failure is different from the runtime failures documented later, so verify that dependency resolution and compilation work before debugging AWS APIs.

For a Mac using Apple Silicon, choose `Dockerfile.arm64`; for an Intel Mac or AMD64 Linux system, choose `Dockerfile.amd64`. On a system whose architecture is uncertain, `uname -m` returns a useful indicator: `arm64` or `aarch64` indicates ARM64, while `x86_64` indicates the Intel/AMD 64-bit architecture. Docker may be able to emulate a different architecture, but using a native architecture avoids unnecessary overhead and ambiguity.

## 5. Start AWS Emulator with Docker

Open Terminal A and change into the project's root directory. Build the appropriate container image. Because the Dockerfiles download the `LTS/latest` RPM, rebuilding later may install a different emulator release even if the Dockerfile itself is unchanged; record the release when reproducibility matters.

```bash
cd "$HOME/Github/aws-emulator-scala3-lab"

# Apple Silicon / ARM64:
docker build -f Dockerfile.arm64 -t aws-emulator .

# Alternatively, on Intel/AMD64:
# docker build -f Dockerfile.amd64 -t aws-emulator .
```

Then start the image. The `-v` mount shares the emulator's generated configuration and certificate files with macOS, while `-e HOME="$HOME"` makes the paths reported by the emulator correspond to the host home directory. The loopback-only host port binding is intentional and should not be broadened.

```bash
mkdir -p "$HOME/.aws/emulator"

docker run --rm --name cs441-aws-emulator \
  --memory=2g --memory-swap=2g \
  -p 127.0.0.1:9297:9297 \
  -e HOME="$HOME" \
  -e EMULATOR_TELEMETRY=off \
  -v "$HOME/.aws/emulator:$HOME/.aws/emulator" \
  aws-emulator -p 9297
```

Keep Terminal A open while working. The container runs in the foreground, so use Terminal B for the remaining steps. The Docker memory options bound memory use; if the emulator exceeds its limit, Docker can terminate the process and its in-memory resources will be lost. Disabling emulator telemetry is a policy choice for this local laboratory, not a requirement for SDK correctness; AWS documents telemetry behavior separately.

The `--name` option gives the container a predictable name, which is helpful when checking `docker ps` or stopping it later. If Docker reports that this name is already in use, inspect existing containers with `docker ps -a` rather than starting a second emulator on the same port. If the port is already bound, identify the process before stopping it; do not assume that every process listening on 9297 is disposable.

For Linux Docker, AWS recommends also adding `--user "$(id -u):$(id -g)"` so generated files have the correct ownership. For Windows, use the Docker PowerShell command in the official AWS documentation and mount the host emulator directory to `/home/emulator/.aws/emulator` in the container. The included `*.sh` scripts target macOS and Linux shells; Windows users can translate their environment settings into `$env:VARIABLE = "value"` assignments or use a compatible Bash shell with correctly mapped paths.

### 5.1 Where the emulator writes local files

The emulator creates a directory based on its port. In this lab, the important generated files are:

```text
$HOME/.aws/emulator/9297/
├── emulator_credentials       AWS-style keys for this emulator run
├── config                     AWS client configuration for this emulator
├── ca.pem                     Certificate authority for curl / AWS CLI
└── truststore.p12             Certificate trust store for Java / Scala
```

These are runtime artifacts, not source files and not part of the ZIP archive. The emulator creates fresh keys and certificates when it starts and normally deletes its per-run files on clean shutdown. Do not commit any generated credentials or certificates to Git. Refer to these files using paths rather than copying their contents into the repository or IntelliJ settings.

## 6. Verify a local-only CLI session

Open Terminal B and run the following commands. The `source` command is crucial: it evaluates the environment script in the *current* Bash or Zsh session, allowing that shell and its child processes to inherit the variables. Running `bash scripts/emulator-env.sh` in a separate subprocess would not configure the parent shell.

```bash
cd "$HOME/Github/aws-emulator-scala3-lab"
source scripts/emulator-env.sh
scripts/check-emulator.sh
```

A passing check first obtains a successful HTTPS health response from `https://127.0.0.1:9297/__admin/health` and then confirms that STS returns account `000000000000`. The check should display a final confirmation like:

```text
ok
Emulator verified. Account: 000000000000
```

This is expected output, not a verbatim transcript from every emulator release. If an error occurs, do not proceed to resource-creation commands. Correct the local endpoint, file paths, or emulator startup state first.

You can inspect the key settings without exposing secrets:

```bash
printf 'endpoint=%s\n' "$AWS_ENDPOINT_URL"
printf 'region=%s\n' "$AWS_REGION"
printf 'credentials=%s\n' "$AWS_SHARED_CREDENTIALS_FILE"
printf 'CA=%s\n' "$AWS_CA_BUNDLE"
printf 'Java trust store=%s\n' "$AWS_EMULATOR_JAVA_TRUSTSTORE"

ls -l "$HOME/.aws/emulator/9297/"
aws sts get-caller-identity
```

The AWS CLI should show account `000000000000` and an emulator principal, not an actual AWS account. Because `emulator-env.sh` unsets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `AWS_PROFILE`, the SDK can use the emulator's generated credentials rather than existing values from a deployment shell. Avoid sourcing this script inside a terminal intended for real AWS deployment, since it intentionally changes how AWS commands route and authenticate.

### 6.1 Environment variable reference

| Variable | This lab's value or source | Purpose |
| --- | --- | --- |
| `AWS_ENDPOINT_URL` | `https://127.0.0.1:9297` | Routes AWS CLI / SDK traffic to the emulator instead of AWS. |
| `AWS_SHARED_CREDENTIALS_FILE` | `$HOME/.aws/emulator/9297/emulator_credentials` | Selects temporary credentials produced by the running emulator. |
| `AWS_CONFIG_FILE` | `$HOME/.aws/emulator/9297/config` | Selects the emulator's AWS client configuration. |
| `AWS_CA_BUNDLE` | `$HOME/.aws/emulator/9297/ca.pem` | Allows CLI/curl-compatible clients to validate the local HTTPS certificate. |
| `AWS_EC2_METADATA_DISABLED` | `true` | Prevents credentials discovery from the EC2 metadata service. |
| `AWS_DISABLE_HOST_PREFIX_INJECTION` | `true` | Helps avoid service-specific hostname rewriting in tools that honor it. |
| `AWS_REGION` | `us-east-1` | Establishes the Region for clients that consult this setting. |
| `AWS_DEFAULT_REGION` | `us-east-1` | Provides the CLI/default Region setting. |
| `AWS_EMULATOR_JAVA_TRUSTSTORE` | `$HOME/.aws/emulator/9297/truststore.p12` | Project-specific variable read by `build.sbt` to configure its forked JVM. |
| `EMULATOR_TELEMETRY` | `off` passed into the Docker container | Disables emulator usage telemetry in this example. |

Note that `AWS_EMULATOR_JAVA_TRUSTSTORE` is a convenience variable defined for this project, not an AWS SDK standard environment variable. Neither Java nor the AWS SDK automatically uses it. `build.sbt` converts its value into standard JVM trust-store properties when sbt starts the forked program.

## 7. Run all four Scala programs from Terminal

Before the first compile, confirm that the emulator is running in Terminal A and that Terminal B has sourced `scripts/emulator-env.sh`. Compile the project and run the main demonstration:

```bash
cd "$HOME/Github/aws-emulator-scala3-lab"
source scripts/emulator-env.sh
scripts/check-emulator.sh
sbt compile
sbt 'runMain AwsEmulatorLab'
```

`sbt compile` checks syntax, resolves dependencies, and produces class files; it does not contact the emulator. The `runMain` command launches the named entry point in an application JVM and performs the actual HTTP requests. Its success depends on the emulator's current state: the original `AwsEmulatorLab` is designed primarily for a fresh instance and is not automatically rerunnable without cleanup.

The two standalone computational examples can run with no emulator process at all:

```bash
sbt 'runMain TestSelector'
sbt 'runMain RetryPlan'
```

The SQS timing probe does require the emulator. Prefer a fresh run and avoid simultaneously executing other code that consumes its queue:

```bash
sbt 'runMain SqsVisibilityProbe'
```

Because `SqsVisibilityProbe` sleeps for three seconds and depends on message visibility timing, do not treat its specific counts as a definitive proof of AWS compatibility. It is an observation experiment, not a formal fidelity test. As with the main lab, clear old resources or restart the emulator when you require a known initial state.

## 8. Configure IntelliJ IDEA on macOS

The actual development session used IntelliJ IDEA, JDK 21, and the project path `/Users/drmark/Github/aws-emulator-scala3-lab`. IntelliJ was invoking `AwsEmulatorLab` with a direct Java command rather than through `sbt 'runMain ...'`. This distinction explains why sourcing a shell script in Terminal did not automatically fix the original IntelliJ environment error. A GUI application launched separately from Terminal does not necessarily inherit the current Terminal shell's exported variables, and IntelliJ's direct Application configuration does not automatically apply `Compile / run / javaOptions` from `build.sbt`.

### 8.1 Import and compile

Open the project's root directory in IntelliJ as an sbt project. Allow it to import the build, download the dependencies, and index the Scala sources; ensure the Scala plugin and JDK 21 are configured for the module. Select `AwsEmulatorLab` as the main class and verify that IntelliJ uses the desired JDK. If the SDK classes cannot be resolved, investigate the sbt project import and dependency resolution before diagnosing TLS or DynamoDB behavior.

### 8.2 Create or edit the Run configuration

Navigate to `Run` > `Edit Configurations` and select the Application configuration whose main class is `AwsEmulatorLab`. Make sure the working directory is the project root and the module classpath includes the compiled Scala sources and AWS SDK dependencies. In the `Environment variables` field, define the variables below using **absolute paths** on this particular Mac:

```text
AWS_ENDPOINT_URL=https://127.0.0.1:9297
AWS_SHARED_CREDENTIALS_FILE=/Users/drmark/.aws/emulator/9297/emulator_credentials
AWS_CONFIG_FILE=/Users/drmark/.aws/emulator/9297/config
AWS_CA_BUNDLE=/Users/drmark/.aws/emulator/9297/ca.pem
AWS_EC2_METADATA_DISABLED=true
AWS_DISABLE_HOST_PREFIX_INJECTION=true
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
AWS_EMULATOR_JAVA_TRUSTSTORE=/Users/drmark/.aws/emulator/9297/truststore.p12
```

Enter these as separate environment-variable entries using IntelliJ's variable editor. Do not paste the entire block into a field that expects a different syntax. Inspect the inherited environment and remove any real AWS credential overrides, especially `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `AWS_PROFILE`; these must not override the emulator credentials.

### 8.3 Set the JVM trust store correctly

In the same Run configuration, enable `VM options` under `Modify options` if necessary. Enter these **two JVM options**, separated by whitespace:

```text
-Djavax.net.ssl.trustStore=/Users/drmark/.aws/emulator/9297/truststore.p12 -Djavax.net.ssl.trustStoreType=PKCS12
```

The first property identifies the certificate trust-store file. The second property identifies its format and must have the exact value `PKCS12`; it must not include any directory names. These are JVM system properties, not environment variables and not Java program arguments. Keep them in the VM Options field, not the Program Arguments field.

It is important not to concatenate some unrelated path with the trust-store type. One actual failed IntelliJ command contained the malformed property:

```text
-Djavax.net.ssl.trustStoreType=PKCS12/Users/drmark/.m2/repository
```

The correct property is:

```text
-Djavax.net.ssl.trustStoreType=PKCS12
```

The erroneous suffix caused Java to treat the whole string as an unsupported trust-store type and fail to initialize an X.509 trust manager. Changing the SDK version or switching HTTP clients would not solve that configuration error. Once fixed, the program advanced far enough to verify STS identity and read back an S3 object, showing that networking and certificate validation were working for that execution.

### 8.4 Confirm IntelliJ is launching the intended configuration

After pressing Apply and OK, run `AwsEmulatorLab` and inspect the beginning of the Java command in IntelliJ's Run window. It should contain both properties as distinct tokens before `-classpath`, followed by the main class name `AwsEmulatorLab`. The list of JARs is a normal classpath and is not itself an error; it is not necessary to edit those JAR paths manually.

To verify the JVM properties from inside the program, temporarily insert the following diagnostic lines at the start of `main` before constructing the SDK clients:

```scala
println("Trust store: " + System.getProperty("javax.net.ssl.trustStore"))
println("Trust store type: " + System.getProperty("javax.net.ssl.trustStoreType"))
println("AWS endpoint: " + sys.env.getOrElse("AWS_ENDPOINT_URL", "(missing)"))
```

The expected values are the generated PKCS12 path, `PKCS12`, and `https://127.0.0.1:9297`. Remove the diagnostics when the configuration has been confirmed. Do not print temporary secret-access keys, complete credential files, or other sensitive values during troubleshooting.

### 8.5 Why running from sbt differs from a direct IntelliJ run

The following lines of `build.sbt` set JVM options for sbt's forked `run` process:

```scala
Compile / run / fork := true,
Compile / run / javaOptions ++= sys.env
  .get("AWS_EMULATOR_JAVA_TRUSTSTORE")
  .toSeq
  .flatMap(path => Seq(
    s"-Djavax.net.ssl.trustStore=$path",
    "-Djavax.net.ssl.trustStoreType=PKCS12"
  )),
```

This configuration reads the environment of the sbt process and passes the trust-store options to a separate Java process. It deliberately avoids applying the generated trust store to sbt itself, because the emulator's trust store replaces Java's normal trust store and may otherwise break HTTPS downloads from public Maven repositories. In contrast, IntelliJ's direct Java invocation requires its own VM options; it does not infer them from the above sbt task. You can use either launch method, but you must configure the one you actually run.

## 9. Source-level walkthrough of AwsEmulatorLab.scala

### 9.1 Fail-closed endpoint and credentials checks

At the beginning of the source, the code extracts `AWS_ENDPOINT_URL` using `sys.env.getOrElse`. If the environment variable does not exist, the program throws `IllegalStateException("Set AWS_ENDPOINT_URL to the local emulator")`. If the variable exists but does not exactly match `https://127.0.0.1:9297`, a `require` check rejects it. This is stricter than merely trusting a configuration file, because changing the environment to a real AWS endpoint cannot silently redirect the lab's destructive operations.

The code next requires `AWS_SHARED_CREDENTIALS_FILE` and checks that its path points to an ordinary file. The SDK's `ProfileCredentialsProvider.create()` then reads the default AWS credentials profile through the SDK's standard configuration mechanism. The source creates an STS client, an S3 client with `.forcePathStyle(true)`, a DynamoDB client, and an SQS client; all use the same explicit local URI and `Region.US_EAST_1`. Resources are closed in a `finally` block so normal execution and exceptions both release the client's underlying HTTP resources.

The first API call obtains the STS caller identity. The program prints the identity only after checking that the account is `000000000000`. This protects the educational exercise against some accidental misrouting, although the safeguard is not a cryptographic attestation of the emulator's binary or a substitute for isolating the application from real cloud credentials.

### 9.2 S3 round trip

The S3 segment has three operations: create a bucket, put an object, and get the object. It uses the following concrete values:

```text
Bucket:      cs441-emulator-lab-001
Object key:  papers/p-001.json
Media type:  application/json
Payload:     {"id":"p-001","title":"Cloud Testing Lab","year":2026}
```

The **bucket** is the logical container for objects, and the **key** is the object's name within that container. `contentType("application/json")` supplies metadata indicating that the object is JSON. The program converts the JSON string into a request body, sends it to the emulator, retrieves it as bytes, converts the bytes back into a UTF-8 string, and asserts exact equality. This checks a concrete round trip, but it does not validate the JSON schema, simulate S3 replication, test object locking, or prove that the service reproduces all S3 error behaviors.

The expected observation, already obtained in the real macOS run, was:

```text
[identity] confirmed emulator account 000000000000
[s3] cs441-emulator-lab-001/papers/p-001.json = {"id":"p-001","title":"Cloud Testing Lab","year":2026}
```

This partial success is diagnostic evidence: the application found its endpoint and credentials, completed an HTTPS request using its Java trust store, and exercised the emulated S3 read/write path. It does not mean the later DynamoDB or SQS operations succeeded in that same execution.

### 9.3 DynamoDB table creation and conditional write

The DynamoDB table is named `CS441Ingest` and declares one partition key, `id`, of scalar type `S`, meaning a string. The `HASH` key type identifies the partition key, not a cryptographic digest supplied by the application. The table uses `PAY_PER_REQUEST` billing mode in its requested configuration; within the emulator this does not create a real AWS bill. After creating the table, the program invokes a DynamoDB waiter to wait until the table exists before attempting to insert an item.

The inserted item's three attributes are:

| Attribute | DynamoDB type | Concrete value | Purpose |
| --- | --- | --- | --- |
| `id` | `S` (string) | `p-001` | Uniquely identifies the paper in this table. |
| `status` | `S` (string) | `PENDING` | Records that the downstream indexing work has not been marked complete. |
| `version` | `N` (number) | `0` | A possible basis for optimistic version control; this lab does not increment it. |

The first `PutItem` uses the condition `attribute_not_exists(id)`, meaning no item with the key `p-001` may already be stored. The subsequent `GetItem` sets `.consistentRead(true)`, requesting a strongly consistent read of that item. The program verifies that the status equals `PENDING` and prints the retrieved version. Finally, it submits the same conditional `PutItem` again and expects `ConditionalCheckFailedException`; if the duplicate write unexpectedly succeeds, the program throws an assertion error.

A correctly completed DynamoDB segment would therefore print messages similar to:

```text
[dynamodb] id=p-001, status=PENDING, version=0
[dynamodb] duplicate conditional write correctly rejected
```

The code does not currently define a retry strategy for table creation, an idempotent initialization function, a transaction covering S3 and DynamoDB, or any state transition from `PENDING` to `COMPLETE`. It also does not verify that a preexisting table has the expected schema. These distinctions matter when evolving the demonstration into a genuine ingestion application.

### 9.4 SQS send, receive, and acknowledge

The SQS segment creates the queue `cs441-indexing-events` and sends one message with the body:

```json
{"id":"p-001","action":"index"}
```

The application requests up to one message with `ReceiveMessage` and checks that the returned body exactly matches the sent string. It then deletes that received message using its receipt handle and prints an acknowledgement message. A receipt handle is an identifier for a particular receipt of a message, not the business identifier `p-001` and not the original message body.

The expected final segment is:

```text
[sqs] received: {"id":"p-001","action":"index"}
[sqs] acknowledged message using its receipt handle
LAB PASS: S3, DynamoDB, and SQS round trips completed
```

The queue message represents a request to index the paper, but there is no actual indexing worker in this repository. The sample deletes the message immediately after checking its body, so it does not demonstrate a completed search index, a fully reliable processing pipeline, or an exactly-once delivery guarantee. Additionally, short polling and emulator-specific timing behavior can affect message receipt; the separate visibility probe studies that distinction more directly.

## 10. A concrete execution trace and the distributed transaction boundary

Consider the intended logical workflow for document `p-001`. Before execution, let the relevant emulated state be `S₀ = (O = absent, D = absent, Q = empty)`, where `O` represents the S3 object, `D` represents the DynamoDB item, and `Q` represents the set of SQS messages for this paper. The program aims to reach a state in which the S3 object exists, the DynamoDB item has status `PENDING`, and an SQS indexing instruction has been delivered to a consumer. These are three independently observable pieces of service state, not one atomic database record.

| Step | API or operation | Relevant state after success | What can go wrong |
| --- | --- | --- | --- |
| 0 | `GetCallerIdentity` | No resource change | Wrong endpoint, credentials, or certificate. |
| 1 | `CreateBucket` | S3 bucket exists | Bucket may already exist. |
| 2 | `PutObject` | `O = present` | Object upload may fail after bucket creation. |
| 3 | `GetObject` | `O = present` | Retrieved bytes may differ or be unavailable. |
| 4 | `CreateTable` | DynamoDB table exists | Table may already exist, as observed in the real run. |
| 5 | `WaitUntilTableExists` | Table is reported ready | Creation may be delayed or a waiter may time out. |
| 6 | Conditional `PutItem` | `D = PENDING` | Same key may already exist, producing a conditional failure. |
| 7 | `GetItem` | Read-back sees `D = PENDING` | Read or validation may fail. |
| 8 | Duplicate `PutItem` | `D = PENDING` unchanged | An unexpected success violates the test's expectation. |
| 9 | `CreateQueue` | Queue exists | Queue creation may fail or conflict with existing attributes. |
| 10 | `SendMessage` | Queue contains an indexing instruction | Send may fail after the S3 and DynamoDB writes. |
| 11 | `ReceiveMessage` | Message is received / temporarily hidden | Polling might yield no message. |
| 12 | `DeleteMessage` | Received message acknowledged | Delete may fail; the message could reappear. |

Suppose S3 storage succeeds but DynamoDB table creation fails because `CS441Ingest` already exists. The resulting state still contains the S3 object; the program does not undo the upload. More generally, if S3 and DynamoDB succeed but SQS fails, the record can remain `PENDING` without a corresponding indexing request. This is an example of a **partial distributed workflow**, where earlier independently committed effects remain after a later step fails.

A robust production design would explicitly define its invariants and recovery rules. One possible design writes an outbox record in the same DynamoDB transaction as an item status change, then independently publishes the queued work and marks the outbox record as delivered. Consumers should recognize duplicate business identifiers or operation IDs rather than assuming every message is unique. This lab does **not** implement that design; it provides the small, concrete sequence needed to motivate it.

## 11. The three observed runtime failures and their resolutions

The following cases are based on the actual execution reports from the project's IntelliJ session. They illustrate a useful diagnostic principle: a stack trace identifies the stage at which the program stopped, and each success printed before that stage narrows the set of possible causes. Fix the first meaningful exception and rerun; do not treat every subsequent exception as a recurrence of an earlier issue.

### 11.1 Failure A: AWS_ENDPOINT_URL missing in IntelliJ

The initial run failed with:

```text
Exception in thread "main" java.lang.IllegalStateException:
Set AWS_ENDPOINT_URL to the local emulator
    at AwsEmulatorLab.scala:23
```

The program compiled and launched, but its environment did not contain the endpoint variable. The shell script had to be sourced in the execution shell, or the equivalent variable had to be added to IntelliJ's **Run configuration**. Setting `AWS_ENDPOINT_URL` in a Terminal window did not alter the environment of an independently launched IntelliJ process. Once the Run configuration had the variable, execution advanced past line 23.

Resolution: set `AWS_ENDPOINT_URL=https://127.0.0.1:9297` in the actual process launcher, along with the remaining emulator variables and temporary credential paths. The example's hard-coded `require` will also refuse an endpoint such as `http://localhost:9297`, `https://localhost:9297`, or any public AWS endpoint. This strictness is intentional; adjust the source deliberately if adapting the lab to a different isolated test port.

### 11.2 Failure B: Malformed Java PKCS12 property

The next run reached an AWS SDK HTTP request but failed during the TLS handshake:

```text
software.amazon.awssdk.core.exception.SdkClientException:
Unable to execute HTTP request: (certificate_unknown)
No X509TrustManager implementation available

Caused by: javax.net.ssl.SSLHandshakeException:
(certificate_unknown) No X509TrustManager implementation available
```

The launch command revealed the concrete cause. Instead of passing the exact trust-store type `PKCS12`, IntelliJ passed `PKCS12/Users/drmark/.m2/repository`, combining the type name with an unrelated directory path. Java could not initialize a compatible X.509 trust manager, so the SDK retried the failing HTTP request but could not complete a TLS handshake. The root cause was the malformed JVM option, not a failure of DynamoDB, S3, Scala syntax, or the AWS account.

Resolution: put the following two options in IntelliJ's VM Options field, separated by a space, and check the generated Java command before running:

```text
-Djavax.net.ssl.trustStore=/Users/drmark/.aws/emulator/9297/truststore.p12 -Djavax.net.ssl.trustStoreType=PKCS12
```

On a separate machine, replace `/Users/drmark` with that machine's actual home directory. Make sure `truststore.p12` exists, and restart the Java application after a new emulator process generates new certificates. Avoid the tempting workaround of disabling HTTPS certificate verification; the objective of this exercise is to validate realistic, correctly configured secure client traffic.

### 11.3 Failure C: DynamoDB table already exists

After correcting TLS, the program successfully identified the emulator and completed the S3 round trip. It then raised:

```text
software.amazon.awssdk.services.dynamodb.model.ResourceInUseException:
Table already exists: CS441Ingest
(Service: DynamoDb, Status Code: 400)
    at AwsEmulatorLab.scala:67
```

Unlike the first two exceptions, this was a response from the DynamoDB service interface, not a local configuration or TLS error. The running emulator already contained a table named `CS441Ingest`, but the original program tried to create it unconditionally. This is expected DynamoDB-style resource-conflict behavior; the stack trace alone does not establish which earlier command or process created the table. The most likely explanation in an interactive lab is leftover state from a prior or partial run, but the corrective action depends only on the fact that the table exists.

Resolution A, quickest when the existing table's data are disposable: close the main application, confirm the emulated account, delete the table using the AWS CLI, wait until it is gone, and rerun the lab. Example:

```bash
cd "$HOME/Github/aws-emulator-scala3-lab"
source scripts/emulator-env.sh
scripts/check-emulator.sh

aws dynamodb delete-table --table-name CS441Ingest
aws dynamodb wait table-not-exists --table-name CS441Ingest
```

This is the targeted cleanup previously recommended for that specific exception. It addresses the existing table, but **does not reset S3 or SQS**. On a subsequent run, an already existing bucket or queue can still produce different results depending on the emulator's implementation and current attributes. Thus, a completely clean rerun is preferable for reproducible acceptance testing.

Resolution B, simplest for an entirely disposable local instance: stop the emulator with Ctrl+C in Terminal A, start a fresh instance using the Docker command in Section 5, source the environment again in Terminal B, and restart the IntelliJ process. AWS Emulator normally stores resources in memory, so a new emulator process begins without the old table, object, or queue. Generated credentials and certificates are recreated, so a currently running Java process must not be reused with stale TLS and credentials.

Resolution C, appropriate for a reusable program: modify initialization to check whether the table already exists, verify its schema, and create it only when absent. Separately, handle the `p-001` conditional write in a controlled way. If an existing item exactly represents the desired initialized state, the application can explicitly decide to treat that as “already initialized”; if it has incompatible content or an unexpected version, the application should fail rather than silently overwrite it. The original Scala code has **not** been changed in this documentation-only revision, so a second run can still raise `ResourceInUseException` or `ConditionalCheckFailedException` until code or state changes are made.

### 11.4 Why skipping CreateTable is not sufficient

Imagine that a previous run inserted the row `(id = p-001, status = PENDING, version = 0)` before a later operation failed. A modification that simply catches `ResourceInUseException` and proceeds would then attempt the conditional `PutItem` for `p-001` again. The condition `attribute_not_exists(id)` would be false, and DynamoDB would correctly throw `ConditionalCheckFailedException`. In contrast, the deliberately repeated conditional write *inside the successful first run* is supposed to throw that same exception and is caught by the code as a positive test result.

The distinction is whether the exception occurs during initialization or during the designed duplicate-check experiment. For idempotent initialization, the code needs a defined policy for an existing resource and an existing item. For a demonstration of conditional uniqueness, the code needs to preserve the expected duplicate rejection. Conflating these two stages can hide data corruption or cause false claims that a distributed write is exactly-once.

### 11.5 General troubleshooting decision table

| Symptom | Stage reached | Most relevant diagnosis | First action |
| --- | --- | --- | --- |
| `Set AWS_ENDPOINT_URL to the local emulator` | Scala startup | Environment variable absent in actual launcher | Configure IntelliJ or source `emulator-env.sh` in the launching shell. |
| `Refusing any endpoint other than the local lab` | Scala startup | Endpoint differs from exact allowed URL | Correct `AWS_ENDPOINT_URL` to `https://127.0.0.1:9297`. |
| Credentials file missing | Scala startup | Emulator is not running or generated files are not mounted | Check container, volume mount, and per-port directory. |
| `Connection refused` or health check fails | Network / listener | Emulator absent, crashed, or wrong port | Inspect `docker ps` and the emulator terminal. |
| `certificate_unknown` / `No X509TrustManager` | TLS handshake | Malformed JVM trust-store configuration | Inspect exact VM options and generated trust-store file. |
| `PKIX path building failed` | TLS validation | Missing, stale, or mismatched trust anchor | Inspect generated certificate, restart JVM, use current trust store. |
| Authentication or signature error | API authentication | Stale or wrong keys, real AWS credentials, or wrong profile | Clear credential overrides, source current emulator environment. |
| Wrong STS account | Identity verification | Not the intended local emulator | Stop; do not perform mutating operations. |
| `ResourceInUseException: Table already exists` | DynamoDB control plane | Reused table name in persistent process state | Reset emulator or delete and wait for the table. |
| `ConditionalCheckFailedException` at first insertion | DynamoDB data plane | Item with `id=p-001` already exists or condition false | Clean state or implement explicitly validated idempotent setup. |
| `ConditionalCheckFailedException` in duplicate-check catch | DynamoDB data plane | Expected second-write rejection | Treat as success when caught by the sample code. |
| `NotImplemented` / HTTP 501 | Emulated API dispatch | Operation unavailable in installed release | Consult release-specific fidelity evidence; don't call it a Scala failure. |
| SQS receive returns no messages | Messaging / timing | Polling, visibility, competing consumer, or fidelity gap | Isolate queue, reset state, recheck polling behavior. |
| Tests work in Terminal but fail in IntelliJ | Process setup | Different environment or JVM flags | Compare launchers, trust-store properties, and credential paths. |

## 12. Safe cleanup and reproducible reruns

A reproducible test needs a known starting state. There are two valid approaches: use a new emulator process for each isolated test, or implement explicit setup and teardown that verify resource state and remove disposable resources. Because the current sample intentionally uses fixed names, the fastest and least ambiguous procedure for repeating the full demonstration is a fresh emulator process. This recommendation is for disposable classroom data only; never apply it to a system containing data that must be retained.

### 12.1 Preferred reset: restart the local emulator

1. Stop `AwsEmulatorLab` or any other client that might still be sending requests.
2. Press Ctrl+C in Terminal A, which runs the Docker container.
3. Start the emulator again with the command in Section 5.
4. In Terminal B, source `scripts/emulator-env.sh` again and run `scripts/check-emulator.sh`.
5. Start a **new** IntelliJ Java process so that it reads the emulator's current credentials and certificates.
6. Run `AwsEmulatorLab` once and retain the full output for the lab report.

Although the file paths stay the same, their contents change across emulator processes. In particular, a new `truststore.p12` contains a new local trust anchor and `emulator_credentials` contains new signing keys. A running Java process that initialized its trust manager or credential provider before the emulator restart might retain stale material. Restarting the application is therefore more reliable than trying to reuse already created SDK clients.

### 12.2 Targeted reset without stopping the emulator

When the emulator contains other valuable *test* resources that must be preserved, delete only the specific lab resources. First run `source scripts/emulator-env.sh` followed by `scripts/check-emulator.sh`; do not execute any cleanup if that identity check fails. The following commands refer only to the fixed names in the original source and assume those resources exist:

```bash
cd "$HOME/Github/aws-emulator-scala3-lab"
source scripts/emulator-env.sh
scripts/check-emulator.sh

# Remove the S3 object before removing its bucket.
aws s3api delete-object \
  --bucket cs441-emulator-lab-001 \
  --key papers/p-001.json
aws s3api delete-bucket --bucket cs441-emulator-lab-001

# Remove the table and wait for the deletion to become observable.
aws dynamodb delete-table --table-name CS441Ingest
aws dynamodb wait table-not-exists --table-name CS441Ingest

# Remove the queue by resolving its current URL.
QUEUE_URL="$(aws sqs get-queue-url \
  --queue-name cs441-indexing-events \
  --query QueueUrl --output text)"
aws sqs delete-queue --queue-url "$QUEUE_URL"
```

If a resource was never created, its deletion may report a not-found error; inspect the state and skip that specific deletion rather than blindly ignoring all failures. The optional `SqsVisibilityProbe` creates a different queue named `cs441-visibility-probe`, which can be removed separately when no longer needed. Deleting a queue discards its messages; deleting a DynamoDB table discards its items; deleting an S3 object discards those bytes. Do not use these cleanup commands against actual AWS, and do not assume they generalize safely to versioned S3 buckets or production accounts.

### 12.3 Improving the program instead of resetting state

A production-quality initializer might implement the following algorithm:

```text
Inputs:
  expected table name = CS441Ingest
  expected partition key = id, scalar type S
  expected item = (id=p-001, status=PENDING, version=0)

1. DescribeTable(CS441Ingest).
2. If the table is absent, issue CreateTable with the expected schema.
3. If another process created it concurrently, re-read rather than retry blindly.
4. Wait until the table reaches its usable state.
5. Verify the existing key schema and any other required table attributes.
6. GetItem(id=p-001).
7. If the item is absent, conditionally insert the expected initial record.
8. If another writer wins the race, re-read the item.
9. If the item exists and exactly matches the allowed initialized state,
   report that the resource was already initialized.
10. Otherwise fail with a clear schema or state-conflict error.
11. Run a separate deliberately duplicated conditional write to test uniqueness.

Outputs:
  Verified table schema; verified initial item; or a precise conflict error.
```

This design distinguishes a setup operation from a test assertion. It also demonstrates why a `describe`-then-`create` sequence alone is subject to a race: two processes may observe an absent table simultaneously, and one may receive `ResourceInUseException` after the other creates it. A correct implementation reconciles the resulting state and verifies its attributes, rather than treating every conflict as success. If tests need strict independence, unique resource names per run or a new emulator process are preferable to shared mutable state.

## 13. TestSelector.scala: choosing experiments under a time budget

`TestSelector.scala` demonstrates an exhaustive finite optimization problem entirely in local Scala. It receives a vector of proposed checks, their estimated execution times in minutes, an illustrative impact weight, and a detection probability. A check's estimated benefit is defined as its impact multiplied by its probability of detecting a relevant defect. These are illustrative teaching values, not AWS fidelity scores or empirically calibrated probabilities.

The source defines exactly three candidate checks:

| Check | Time in minutes | Impact | Detection probability | Calculated benefit |
| --- | ---: | ---: | ---: | ---: |
| S3 round trip | 2 | 5.0 | 0.8 | 4.0 |
| DynamoDB conditional write | 3 | 9.0 | 0.5 | 4.5 |
| SQS receipt/visibility | 2 | 6.0 | 0.6 | 3.6 |

The time budget is four minutes. The algorithm enumerates all subsets of the three checks using bit masks from zero through seven, filters out those whose total duration exceeds four minutes, computes the sum of benefits for each feasible subset, and selects the subset with the greatest benefit. With these exact values, the winning plan includes S3 and SQS, because their total time is four minutes and their combined modeled benefit is 7.6. A standalone DynamoDB check has benefit 4.5 but would prevent either two-minute check from fitting into a four-minute budget.

A general expression for the pedagogical objective is `maximize B(T) = Σ bᵢ`, taken over all selected checks `i` in a set `T`, subject to `Σ tᵢ ≤ 4`. Here `Σ` means addition across the selected checks, `bᵢ` is the benefit assigned to check `i`, and `tᵢ` is its duration in minutes. The `maxBy` operation in Scala is the programmatic equivalent of selecting an `argmax`, namely the candidate subset that achieves the greatest objective value. It is not the maximum value itself: it returns the subset, while `maxBy(...).map(_.benefit).sum` would compute that subset's modeled benefit.

The implementation takes no command-line parameters and calls no cloud service. As the number of candidate tests grows, enumerating all `2ⁿ` subsets becomes expensive, where `n` is the number of checks and `2ⁿ` is the number of possible inclusion/exclusion combinations. For three checks there are only eight subsets, so the exhaustive approach is simple and appropriate. A larger test portfolio may require dynamic programming, integer optimization, or a heuristic that accounts for dependencies among tests.

Run it with:

```bash
sbt 'runMain TestSelector'
```

## 14. RetryPlan.scala: classify errors before retrying

`RetryPlan.scala` is a pure policy demonstration. It never contacts AWS Emulator and never injects a network fault. It defines two possible results, `RetryAfter(milliseconds)` and `FailPermanently`, and chooses between them based on an error-code string and a zero-based retry index. The only error-code strings treated as retryable in this example are `ThrottlingException`, `ServiceUnavailable`, and `RequestTimeout`.

The retry policy starts with a delay of 100 milliseconds and doubles that delay for successive retry indices until a maximum of 1,600 milliseconds. If `retryIndex` is four or larger, the function returns `FailPermanently`, so the four retry indices 0, 1, 2, and 3 permit delays of 100, 200, 400, and 800 milliseconds. A nonretryable code such as `ValidationException` fails immediately. These choices illustrate bounded retries, but their values are not universal AWS recommendations and do not model all SDK retry configurations.

The example corresponds to the following expression for permitted retry indices: `delay(i) = min(1600, 100 × 2ⁱ)` milliseconds. Here `i` is the zero-based retry index, `2ⁱ` means two raised to the power `i`, `×` means multiplication, and `min(a,b)` selects the smaller of the two numbers. The predicate “retryable error and i < 4” is checked **before** computing the delay. The `min` expression therefore limits delay size but does not by itself decide whether an operation should be retried.

The distinction between transient and permanent failure is critical in distributed systems. A timeout could indicate that the service never received the request, or that it successfully performed the operation but the response was lost. Retrying a non-idempotent operation without a deduplication key can therefore create duplicate effects. The sample only returns suggested actions; a real retry loop would also enforce an overall deadline, cancellation, backoff jitter, and application-specific idempotency rules.

Run it with:

```bash
sbt 'runMain RetryPlan'
```

## 15. SqsVisibilityProbe.scala: inspect visibility timeout behavior

This optional program creates a queue named `cs441-visibility-probe` with a two-second visibility timeout, sends the message body `probe-441`, and receives it once. It immediately tries to receive another message, then sleeps for three seconds and tries again. The intended observation is that the first receive temporarily hides the message and that the message becomes eligible for delivery again after its visibility timeout if it was not deleted.

A possible output trace is:

```text
t0: received first receipt handle
t0+short: immediately visible count=0
t0+3s: redelivered count=1
Interpret timing and short-polling observations cautiously; check fidelity tests
```

Do not require these exact counts without considering short polling, scheduling, and emulator fidelity. The code records what happened; it does not assert `immediate == 0` or `after == 1`, precisely because timing results may be release-sensitive. The code also does not delete the message or queue at the end, so a later test against the same running emulator can encounter existing state. Perform the probe on an isolated instance if the goal is a clean experimental record.

This example makes an important distinction between service emulation and a fault-injection simulator. The AWS Emulator attempts to implement AWS API behavior, but the companion project does not supply a deterministic event scheduler that can arbitrarily drop packets, partition nodes, or reorder internal SQS events. A comparison with true distributed-systems model checking or controlled fault injection would require additional test infrastructure. An apparent visibility-timeout match is one observed behavior, not proof of complete SQS semantics.

## 16. How to interpret fidelity and emulator limitations

**Fidelity** describes how closely the emulator's behavior matches actual AWS behavior for specific API calls and workflows. AWS publishes service-level scores with emulator releases and provides an evaluation suite whose `manifest.jsonl` maps fidelity tests to services and operations. For a particular operation, the important observations include return values, HTTP status, errors on invalid requests, resource-state changes, and downstream effects. A service can have successful basic CRUD demonstrations while still failing an edge case involving validation, ordering, propagation, or resource interaction.

For the current lab, the first operations worth checking are STS `GetCallerIdentity`, S3 `CreateBucket`/`PutObject`/`GetObject`, DynamoDB `CreateTable`/`DescribeTable`/`PutItem`/`GetItem`, and SQS `CreateQueue`/`SendMessage`/`ReceiveMessage`/`DeleteMessage`. Next, inspect conditional-write behavior, handling of existing resources, and the visibility-timeout experiment. A broad service-level score is not a substitute for knowing which exact workflows are covered. If a behavior has no published fidelity test, absence of coverage is not evidence that the emulator implements it incorrectly, but it also supplies no independent confirmation of a match.

When conducting differential tests, execute logically equivalent scenarios against an isolated emulator and an explicitly budgeted real AWS test account. Normalize only irrelevant differences such as random request identifiers, and compare externally meaningful results and errors. Always tag the emulator release and evaluation-suite version because scores and implementations can change over time. Do not run a real-AWS experiment simply by removing `AWS_ENDPOINT_URL` from this lab; its fail-closed restriction exists specifically to prevent such accidental redirection.

As a useful mathematical distinction, define `Fᵢ = passedᵢ / totalᵢ` for one service's published test set, where `passedᵢ` is the number of passing published fidelity tests for service `i` and `totalᵢ` is the total number considered for that release. An application-level confidence argument requires something different: evidence that the particular behaviors exercised by the application have been checked and that important missing behaviors are understood. A high average score across services cannot logically imply that a specific untested cross-service workflow is correct.

The AWS Emulator may return HTTP 501 and a `NotImplemented` error for unsupported operations. It has known issues documented in the public upstream repository, including the `issues/things-that-do-not-work/` directory. Treat such responses as gaps in emulation coverage, not as proof that Scala or real AWS is broken. If you report a difference, include the emulator version, exact operation, sanitized input, observed response, expected AWS behavior, and a short reproducer without credentials or personal data.

## 17. Security boundary, costs, and safe engineering practice

The emulator is designed for development and functional testing, not for production processing or validating AWS security controls. In particular, passing tests do not establish correct IAM permissions, S3 bucket policies, KMS key policies, cross-account authorization, or cross-Region behavior. AWS states that the emulator may allow operations that production AWS would reject, and vice versa. Therefore, students must test actual deployment permissions and security-sensitive behaviors in a separately controlled AWS environment before making any production claim.

Local resources created in AWS Emulator do not themselves incur AWS service charges, and this sample does not require an AWS account. Docker, CPU cycles, memory, disk space, and developer time still have costs, and any later tests deployed to real AWS can incur real bills. No “free emulator” claim should be interpreted as unlimited free AWS usage. If the project is later adapted for a course assignment with a spending limit, all real cloud resources must be budgeted, monitored, and cleaned up independently of this local lab.

Keep the host port bound to `127.0.0.1` and do not disable TLS verification to bypass certificate errors. Do not share real credentials with the emulator or use real customer data as test payloads. Some emulated services may execute code supplied by an application; AWS warns that this code is not necessarily isolated from the host user's permissions. Run only code you trust, and do not interpret the emulator's signature checks as complete production-grade access-control enforcement.

The generated `truststore.p12` can also have side effects if used globally. Java's `javax.net.ssl.trustStore` setting replaces the default JVM trust store rather than adding the emulator's certificate to it. As a result, a Java process that also contacts public Maven repositories or other HTTPS services may fail certificate validation. This project deliberately configures only sbt's forked application JVM so sbt can continue using its normal trust configuration for dependency downloads; IntelliJ's direct application VM options serve the same purpose.

## 18. Advanced extension: AWS agents, Smithy, and evidence-gated development

Werner Vogels and Swaminathan Sivasubramanian explain the motivation for AWS Emulator in their November 11, 2026 article. They connect contemporary API emulation with earlier efforts to test the Alf distributed locking and replication system through a controllable simulation environment and formal reasoning. The new challenge is breadth: AWS exposes many services and API operations, and software written by coding agents must be checked before it is accepted. The article argues that engineers increasingly design specifications, evidence-producing tests, and merge gates that determine whether agent-generated implementations are correct.

**Smithy** is a description language for modeling services, API operations, input and output structures, and errors. An API model can supply a coding agent with a contract for the format of a request, but it cannot alone prove every observable service behavior, such as message visibility, retries, or complex state transitions. AWS Emulator's development process therefore uses differential fidelity tests that capture expected behaviors from AWS and checks whether the emulator reproduces them. This project is a small consumer of that philosophy: it treats the local emulator as a testable approximation, not an infallible oracle.

A useful extension for a research or advanced CS441 assignment is to build a machine-readable table mapping each SDK call in `AwsEmulatorLab.scala` to its input, expected output, persistent state change, and published fidelity evidence. Students can then design negative tests for duplicate resources, invalid request shapes, mismatched S3 object bytes, expired SQS visibility, and concurrent DynamoDB writes. A stronger extension would implement safe, deterministic cleanup and idempotent reinitialization, then evaluate whether rerunning the application yields the same intended state. Each proposed claim should identify what was observed, which emulator release produced the observation, and which behavior remains untested.

The AWS CLI also offers an optional agent toolkit for installing an `aws-emulator` skill into compatible AI coding assistants. The official command requires AWS CLI version 2.35.0 or later:

```bash
aws --version
aws agent-toolkit add-skill --skill-name aws-emulator --region us-east-1
```

That agent workflow is optional and does not change how this Scala project connects to its emulator process. An agent's successful execution is still not independent proof that an implementation is correct; the engineer must inspect the test oracle, environmental assumptions, and the evidence that accompanies the result. Similarly, the emulator project's own AI-written implementation is not evidence that every API is complete, so service-specific fidelity and real-AWS validation remain important.

## 19. Reproducible classroom or CI procedure

A trustworthy automated test should reproduce its environment rather than inherit unknown state from a developer's previous interactive experiments. For this original sample, the simplest strategy is to start a fresh emulator instance, run the health and identity gates, compile the project, run the main laboratory exactly once, and collect the log. The release of the emulator package, Scala version, AWS SDK version, Java version, OS/architecture, and command output should be saved with the experiment. Sensitive credentials and generated trust-store contents should never be included in logs or submitted artifacts.

A shell-oriented procedure is:

```bash
# Terminal A: build once, then start an isolated emulator.
docker build -f Dockerfile.arm64 -t aws-emulator .
mkdir -p "$HOME/.aws/emulator"
docker run --rm --memory=2g --memory-swap=2g \
  -p 127.0.0.1:9297:9297 \
  -e HOME="$HOME" -e EMULATOR_TELEMETRY=off \
  -v "$HOME/.aws/emulator:$HOME/.aws/emulator" \
  aws-emulator -p 9297

# Terminal B: run only after Terminal A reports readiness.
source scripts/emulator-env.sh
scripts/check-emulator.sh
sbt compile
sbt 'runMain AwsEmulatorLab'
sbt 'runMain TestSelector'
sbt 'runMain RetryPlan'
```

For CI, the corresponding runner needs to install or expose Docker and AWS CLI, start the container without attaching its foreground terminal, wait for the HTTPS health check, then source the generated environment in the same job step that launches sbt. Do not launch two copies of the main lab against one shared set of fixed resource names. On failure, preserve sanitized program output and the emulator release identifier, then stop and remove the disposable container in a final cleanup step.

### 19.1 Suggested experiment report

A concise reproducibility report should state the hypothesis, assumptions, emulator release, code revision, command, expected behavior, actual behavior, and interpretation. For example, the hypothesis for DynamoDB conditional writes is that the first insert of `id=p-001` succeeds and the second identical conditional insert is rejected. The independent variable is whether the item already exists; the dependent variable is whether the call succeeds or returns `ConditionalCheckFailedException`. A valid conclusion is limited to that operation under the tested conditions and does not assert full transactional semantics across S3, DynamoDB, and SQS.

A separate experiment can test setup repeatability. Run the main application once, without deleting resources run it again, and record where the second run fails. Next, restart the emulator and run the application again, comparing the outcomes. This contrast exposes state contamination in repeatable integration testing and motivates either per-test isolation or properly designed idempotent setup and teardown.

## 20. Current status and what remains to be improved

The available execution evidence confirms that, after configuring IntelliJ's environment and correcting its JVM trust-store options, `AwsEmulatorLab` reached STS and printed the correct local account identity. The same execution successfully stored and retrieved the S3 JSON document. It then failed while creating `CS441Ingest` because that table already existed in the emulator's state. No later success for DynamoDB conditional writes or SQS processing was established by that reported run.

The source code in this companion archive remains intentionally unchanged. It still creates fixed names without a complete initial-state reconciliation algorithm, uses one conditional item insert that should succeed only when `p-001` is absent, and contains a separate deliberate duplicate-write test. It still does not implement a worker, outbox, transaction coordinator, comprehensive cleanup, or deterministic fault injection. These are legitimate limitations of a teaching lab and are explicitly documented rather than being presented as implemented features.

For the next engineering iteration, prioritize (1) a safe, separately testable local-only client factory, (2) idempotent resource setup with schema validation, (3) a cleanup mechanism that is reliably gated by endpoint and identity checks, (4) unique resource names or isolated emulator processes per test, (5) assertions for all intended state transitions, and (6) a small published-fidelity checklist for the exact SDK calls used. Preserve the current duplicate conditional-write test as a distinct experiment so that making the setup reusable does not accidentally remove the behavior it was designed to teach.

## 21. Frequently asked questions

**Why does the program need a local AWS URL if its API code already uses AWS SDK clients?** The SDK knows how to construct AWS requests, but it must still know where to send them. Without an explicit local endpoint, many SDK configurations would default to public AWS endpoints. The endpoint guard is therefore a required safety check, not a cosmetic configuration detail.

**Why does STS succeed while DynamoDB fails?** Each service operation has its own validation and persistent state. A valid STS identity confirms that the program reached a compatible identity interface, not that every DynamoDB resource is absent or correctly configured. In the observed run, the emulator's DynamoDB interface returned an application-level conflict because `CS441Ingest` already existed.

**Why does the emulator need credentials if it runs on my laptop?** Its default API behavior includes signed requests, which helps protect the local service from unintended unsigned calls and makes client behavior more realistic. The credentials are generated for the emulator process and are not associated with a billable cloud account. Treat them as temporary test credentials, and avoid mixing them with actual AWS deployment credentials.

**Why not turn off TLS verification to make the test work?** Doing so would hide a meaningful configuration error and reduce the exercise's realism. The correct solution is to trust the emulator's generated certificate using `ca.pem` or `truststore.p12` as appropriate. The actual TLS error in this project was solved by correcting the malformed JVM trust-store type.

**Why does the second run fail after the first run worked?** The main program uses constant names, whereas the emulator retains resources across calls until its process ends. Repeated `CreateTable` calls can conflict, and repeated conditional `PutItem` calls can violate the insert-if-absent precondition. Restart for isolated lab runs, delete disposable resources after verifying the endpoint, or implement explicit idempotent setup.

**Is a successful emulated SQS message receipt proof of exactly-once processing?** No. Receiving and deleting a message demonstrates selected API behavior in one run; exactly-once end-to-end effects require application-specific design and proof. A consumer can crash after taking a side effect but before acknowledgement, so deduplication and recovery remain necessary.

**Can the same application be deployed to real AWS by changing one environment variable?** Not safely in the form provided. The source deliberately rejects nonlocal endpoints, and its resource names, IAM permissions, retry choices, cleanup behavior, and budget controls are not production-ready. A separate configuration and security review is needed before any actual AWS execution.

**Does this project use Rust or compile the emulator itself?** No. The companion code is Scala 3 and depends on Java SDK artifacts. The Dockerfiles install AWS's prebuilt Linux emulator package into a container; they do not build the upstream Rust source workspace.

**Will a successful `sbt compile` prove my emulator works?** No. Compilation verifies source syntax and types but performs none of the AWS API calls in `AwsEmulatorLab`. The health check, identity check, and service operations are runtime checks and must be examined separately.

## 22. Source documents and further reading

The authoritative external references for this project are:

1. [AWS Emulator GitHub repository](https://github.com/aws/aws-emulator) — source layout, launch guidance, known issue directories, and release history.
2. [AWS Emulator user guide](https://docs.aws.amazon.com/emulator/latest/userguide/what-is-service.html) — product behavior and official navigation.
3. [Getting started with Docker](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-docker.html) — architecture-specific RPM container construction, volume mounts, and loopback port binding.
4. [Pointing your AWS SDK at AWS Emulator](https://docs.aws.amazon.com/emulator/latest/userguide/point-sdk-to-emulator.html) — environment variables, credentials, TLS, hostname rewriting, and Java trust stores.
5. [Security in AWS Emulator](https://docs.aws.amazon.com/emulator/latest/userguide/security.html) — limitations of security tests and host-protection requirements.
6. [Fidelity in AWS Emulator](https://docs.aws.amazon.com/emulator/latest/userguide/fidelity.html) — fidelity tests, scores, release comparisons, and known gaps.
7. [Checking whether your project works with AWS Emulator](https://docs.aws.amazon.com/emulator/latest/userguide/check-project.html) — mapping individual API operations to tests and evidence.
8. [Telemetry in AWS Emulator](https://docs.aws.amazon.com/emulator/latest/userguide/telemetry.html) — collected data and opt-out controls.
9. [Getting started with an AI coding agent](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-agent.html) — optional CLI agent-toolkit skill.
10. [AWS Emulator and the engineer's job when agents write the code](https://www.allthingsdistributed.com/2026/11/aws-emulator-and-the-engineers-job-when-agents-write-the-code.html), November 11, 2026 — historical motivation, Alf, Paxos, fidelity testing, and evidence-driven agent engineering.
11. [AWS Emulator evaluation suite](https://github.com/aws/aws-emulator-evaluation-suite) — public fidelity test cases and test manifest.

For deeper theory, consult the companion Pressbooks chapter accompanying this repository. It expands the material on observable state machines, distributed systems consistency, mathematical models of fidelity and test selection, failure scenarios, and the limits of agent-written cloud software. The operational instructions in this README, however, should be sufficient to install, verify, run, diagnose, reset, and interpret the supplied Scala 3 experiments without having to consult the chapter during every command.

---

Last verified against this archive's source files: November 18, 2026. Runtime observations included in Section 11 were supplied from an actual macOS/IntelliJ run, while unobserved expected output and proposed future changes are labeled as such. Use a fresh emulator instance, confirm `000000000000`, keep the endpoint local, and treat unexpected errors as evidence to investigate rather than something to suppress.
