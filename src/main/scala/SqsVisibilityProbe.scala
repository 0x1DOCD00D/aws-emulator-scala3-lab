/*
 * Copyright (c) 2026 Mark Grechanik and Lone Star Consulting, Inc.
 * All rights reserved.
 *
 * Project: aws-emulator-scala3-lab
 * File: SqsVisibilityProbe.scala
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
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.*

/**
 * A timing-oriented probe, NOT a definitive AWS equivalence/security test.
 * It runs on a fresh emulator, and its time behavior is release-dependent.
 */
object SqsVisibilityProbe:
  def main(args: Array[String]): Unit =
    val endpoint = sys.env.getOrElse("AWS_ENDPOINT_URL", "")
    require(endpoint == "https://127.0.0.1:9297", "Local emulator required")
    val path = sys.env.getOrElse("AWS_SHARED_CREDENTIALS_FILE", "")
    require(Files.isRegularFile(Paths.get(path)), "Local credentials file required")
    val client = SqsClient.builder()
      .endpointOverride(URI.create(endpoint))
      .region(Region.US_EAST_1)
      .credentialsProvider(ProfileCredentialsProvider.create())
      .build()
    try
      val queueUrl = client.createQueue(CreateQueueRequest.builder()
        .queueName("cs441-visibility-probe")
        .attributes(Map(QueueAttributeName.VISIBILITY_TIMEOUT -> "2").asJava)
        .build()).queueUrl()
      client.sendMessage(SendMessageRequest.builder()
        .queueUrl(queueUrl).messageBody("probe-441").build())
      val first = client.receiveMessage(ReceiveMessageRequest.builder()
        .queueUrl(queueUrl).maxNumberOfMessages(1).waitTimeSeconds(1)
        .build()).messages().asScala.toList
      assert(first.nonEmpty, "Initial receive did not yield a message")
      println("t0: received first receipt handle")
      val immediate = client.receiveMessage(ReceiveMessageRequest.builder()
        .queueUrl(queueUrl).maxNumberOfMessages(1).waitTimeSeconds(0)
        .build()).messages().size()
      println(s"t0+short: immediately visible count=$immediate")
      Thread.sleep(3000L)
      val after = client.receiveMessage(ReceiveMessageRequest.builder()
        .queueUrl(queueUrl).maxNumberOfMessages(1).waitTimeSeconds(1)
        .build()).messages().asScala.toList
      println(s"t0+3s: redelivered count=${after.size}")
      println("Interpret timing and short-polling observations cautiously; check fidelity tests")
    finally client.close()
