/*
 * Copyright (c) 2026 Mark Grechanik and Lone Star Consulting, Inc.
 * All rights reserved.
 *
 * Project: aws-emulator-scala3-lab
 * File: AwsEmulatorLab.scala
 * Author: Mark Grechanik
 * Date: 2026-11-18
 *
 * PROPRIETARY AND CONFIDENTIAL
 *
 * This software, including its source code, documentation, algorithms,
 * and associated materials, is the intellectual property of
 * Mark Grechanik and Lone Star Consulting, Inc.
 *
 * Unauthorized copying, reproduction, modification, distribution,
 * disclosure, sublicensing, reverse engineering, or commercial use,
 * in whole or in part, is prohibited without prior written permission
 * from the respective copyright holders, except as permitted by
 * applicable law.
 *
 * No license, ownership interest, or other intellectual property
 * rights are granted except as expressly authorized in writing.
 *
 * ALL SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT.
 */

import java.net.URI
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.*

/**
 * A fresh AWS Emulator instance is required for each run, because the example
 * intentionally creates fixed, easy-to-read resource names.
 */
object AwsEmulatorLab:
  def main(args: Array[String]): Unit =
    val endpoint = sys.env.getOrElse("AWS_ENDPOINT_URL",
      throw new IllegalStateException("Set AWS_ENDPOINT_URL to the local emulator"))
    require(endpoint == "https://127.0.0.1:9297",
      s"Refusing any endpoint other than the local lab: $endpoint")

    val credentialsPath = sys.env.getOrElse("AWS_SHARED_CREDENTIALS_FILE",
      throw new IllegalStateException("Set AWS_SHARED_CREDENTIALS_FILE"))
    require(Files.isRegularFile(Paths.get(credentialsPath)),
      s"Emulator credentials file is missing: $credentialsPath")

    val creds = ProfileCredentialsProvider.create()
    val region = Region.US_EAST_1
    val url = URI.create(endpoint)
    val sts = StsClient.builder().region(region).endpointOverride(url)
      .credentialsProvider(creds).build()
    val s3 = S3Client.builder().region(region).endpointOverride(url)
      .credentialsProvider(creds).forcePathStyle(true).build()
    val dynamo = DynamoDbClient.builder().region(region).endpointOverride(url)
      .credentialsProvider(creds).build()
    val sqs = SqsClient.builder().region(region).endpointOverride(url)
      .credentialsProvider(creds).build()

    try
      val account = sts.getCallerIdentity().account()
      require(account == "000000000000", s"Unexpected AWS account $account")
      println(s"[identity] confirmed emulator account $account")

      val bucket = "cs441-emulator-lab-001"
      val key = "papers/p-001.json"
      val payload = """{"id":"p-001","title":"Cloud Testing Lab","year":2026}"""
      s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
      s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
        .contentType("application/json").build(), RequestBody.fromString(payload))
      val received = s3.getObjectAsBytes(GetObjectRequest.builder()
        .bucket(bucket).key(key).build()).asUtf8String()
      assert(received == payload, "S3 round trip changed the bytes")
      println(s"[s3] $bucket/$key = $received")

      val table = "CS441Ingest"
      dynamo.createTable(CreateTableRequest.builder()
        .tableName(table)
        .attributeDefinitions(AttributeDefinition.builder()
          .attributeName("id").attributeType(ScalarAttributeType.S).build())
        .keySchema(KeySchemaElement.builder()
          .attributeName("id").keyType(KeyType.HASH).build())
        .billingMode(BillingMode.PAY_PER_REQUEST).build())
      dynamo.waiter().waitUntilTableExists(DescribeTableRequest.builder()
        .tableName(table).build())
      val item = Map(
        "id" -> AttributeValue.builder().s("p-001").build(),
        "status" -> AttributeValue.builder().s("PENDING").build(),
        "version" -> AttributeValue.builder().n("0").build()
      ).asJava
      val write = PutItemRequest.builder().tableName(table)
        .item(item).conditionExpression("attribute_not_exists(id)").build()
      dynamo.putItem(write)
      val loaded = dynamo.getItem(GetItemRequest.builder()
        .tableName(table)
        .key(Map("id" -> AttributeValue.builder().s("p-001").build()).asJava)
        .consistentRead(true).build()).item()
      assert(loaded.get("status").s() == "PENDING")
      println(s"[dynamodb] id=p-001, status=${loaded.get("status").s()}, version=${loaded.get("version").n()}")
      try
        dynamo.putItem(write)
        throw new AssertionError("Duplicate conditional write was accepted")
      catch
        case _: ConditionalCheckFailedException =>
          println("[dynamodb] duplicate conditional write correctly rejected")

      val queueUrl = sqs.createQueue(CreateQueueRequest.builder()
        .queueName("cs441-indexing-events").build()).queueUrl()
      val message = """{"id":"p-001","action":"index"}"""
      sqs.sendMessage(SendMessageRequest.builder()
        .queueUrl(queueUrl).messageBody(message).build())
      val messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
        .queueUrl(queueUrl).maxNumberOfMessages(1)
        .waitTimeSeconds(0).build()).messages().asScala.toList
      assert(messages.nonEmpty, "The expected SQS message was not received")
      assert(messages.head.body() == message)
      println(s"[sqs] received: ${messages.head.body()}")
      sqs.deleteMessage(DeleteMessageRequest.builder()
        .queueUrl(queueUrl)
        .receiptHandle(messages.head.receiptHandle()).build())
      println("[sqs] acknowledged message using its receipt handle")
      println("LAB PASS: S3, DynamoDB, and SQS round trips completed")
    finally
      sqs.close()
      dynamo.close()
      s3.close()
      sts.close()
