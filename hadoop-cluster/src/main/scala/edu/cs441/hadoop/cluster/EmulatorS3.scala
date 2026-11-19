package edu.cs441.hadoop.cluster

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import scala.jdk.CollectionConverters.*

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.core.sync.{RequestBody, ResponseTransformer}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

/**
 * AWS Emulator S3 client; fail closed if the endpoint, credential file, trust store,
 * Region or observed STS account is not precisely the local teaching environment.
 * Docker/Hadoop operations NEVER require AWS credentials.
 */
final class EmulatorS3 private () extends AutoCloseable:
  private val endpoint = URI.create("https://127.0.0.1:9297")
  private val credentials = ProfileCredentialsProvider.create()
  private val sts = StsClient.builder()
    .endpointOverride(endpoint)
    .region(Region.US_EAST_1)
    .credentialsProvider(credentials)
    .build()
  private val s3 = S3Client.builder()
    .endpointOverride(endpoint)
    .region(Region.US_EAST_1)
    .forcePathStyle(true)
    .credentialsProvider(credentials)
    .build()

  def verifyIdentity(): Unit =
    val account = sts.getCallerIdentity().account()
    require(account == "000000000000",
      s"Refusing S3 operations: expected the local emulator account, got $account")
    println(s"[emulator] confirmed local STS identity, account=$account")

  def uploadFile(file: Path, bucket: String, key: String): Unit =
    EmulatorS3.validateDestination(bucket, key)
    require(Files.isRegularFile(file), s"Host file missing: $file")
    // Read before creating so existing buckets are never modified unnecessarily.
    val exists = try
      s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build())
      true
    catch
      case error: S3Exception if error.statusCode() == 404 => false
    if !exists then
      println(s"[s3] creating disposable teaching bucket $bucket")
      s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
    s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
      RequestBody.fromFile(file))
    println(s"[s3] uploaded $file -> s3://$bucket/$key")

  def downloadFile(bucket: String, key: String, destination: Path): Unit =
    EmulatorS3.validateDestination(bucket, key)
    require(!Files.exists(destination), s"Refusing to overwrite host file $destination")
    s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
      ResponseTransformer.toFile(destination))
    println(s"[s3] downloaded s3://$bucket/$key -> $destination")

  /** Upload MapReduce part files plus a manifest under a unique run directory. */
  def uploadResults(resultDirectory: Path, bucket: String, prefix: String,
                    runId: String, hdfsOutput: String): String =
    EmulatorS3.validateDestination(bucket, s"${prefix.stripSuffix("/")}/$runId")
    val target = s"${prefix.stripSuffix("/")}/$runId"
    val children = Files.list(resultDirectory)
    val parts = try
      children.iterator().asScala.toVector.filter(path =>
        Files.isRegularFile(path) && path.getFileName.toString.startsWith("part-"))
    finally children.close()
    require(parts.nonEmpty, s"No Hadoop part files in $resultDirectory")
    parts.foreach(p => uploadFile(p, bucket, s"$target/${p.getFileName.toString}"))

    val manifest = Files.createTempFile("cs441-hadoop-manifest-", ".json")
    val payload =
      s"""{
         |  "computeEnvironment": "Docker Hadoop 3.4.2 / real YARN, not emulated EMR",
         |  "storageEnvironment": "AWS Emulator S3",
         |  "emulatorAccount": "000000000000",
         |  "recordedAt": "${Instant.now()}",
         |  "hdfsOutput": "${EmulatorS3.jsonEscape(hdfsOutput)}",
         |  "resultFiles": ${parts.size}
         |}
         |""".stripMargin
    try
      Files.writeString(manifest, payload)
      uploadFile(manifest, bucket, s"$target/manifest.json")
    finally Files.deleteIfExists(manifest)
    s"s3://$bucket/$target/"

  override def close(): Unit =
    s3.close()
    sts.close()
    credentials.close()

object EmulatorS3:
  private val expectedEndpoint = "https://127.0.0.1:9297"

  def validateDestination(bucket: String, key: String): Unit =
    require(bucket.matches("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"),
      s"Invalid example S3 bucket: $bucket")
    require(key.nonEmpty && !key.startsWith("/") && !key.contains("..") &&
      !key.contains("\\") && !key.contains("\u0000"),
      s"Invalid S3 object key: $key")

  private def jsonEscape(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"")

  private def requireLocalConfiguration(): Unit =
    val env = sys.env
    require(env.get("AWS_ENDPOINT_URL").contains(expectedEndpoint),
      s"Set AWS_ENDPOINT_URL=$expectedEndpoint by sourcing scripts/emulator-env.sh")
    require(env.get("AWS_REGION").contains("us-east-1"),
      "AWS_REGION must be us-east-1 for the lab")

    val root = Paths.get(System.getProperty("user.home"), ".aws", "emulator", "9297")
    val expectedCredentials = root.resolve("emulator_credentials").toAbsolutePath.normalize()
    val suppliedCredentials = env.get("AWS_SHARED_CREDENTIALS_FILE")
      .map(v => Paths.get(v).toAbsolutePath.normalize())
    require(suppliedCredentials.contains(expectedCredentials) &&
      Files.isRegularFile(expectedCredentials),
      s"The emulator credentials file must be $expectedCredentials")

    val expectedStore = root.resolve("truststore.p12").toAbsolutePath.normalize()
    val actualStore = Option(System.getProperty("javax.net.ssl.trustStore"))
      .map(v => Paths.get(v).toAbsolutePath.normalize())
    val storeType = System.getProperty("javax.net.ssl.trustStoreType", "")
    require(actualStore.contains(expectedStore) && Files.isRegularFile(expectedStore) &&
      storeType == "PKCS12",
      "TLS: set -Djavax.net.ssl.trustStore=<home>/.aws/emulator/9297/truststore.p12 " +
      "and -Djavax.net.ssl.trustStoreType=PKCS12 as SEPARATE JVM arguments")

    require(!env.contains("AWS_PROFILE") &&
      !env.contains("AWS_ACCESS_KEY_ID") &&
      !env.contains("AWS_SECRET_ACCESS_KEY") &&
      !env.contains("AWS_SESSION_TOKEN"),
      "Unset global AWS_PROFILE and AWS_* credential environment variables")

  def open(): EmulatorS3 =
    requireLocalConfiguration()
    val adapter = new EmulatorS3()
    try
      adapter.verifyIdentity()
      adapter
    catch
      case e: Throwable =>
        adapter.close()
        throw e
