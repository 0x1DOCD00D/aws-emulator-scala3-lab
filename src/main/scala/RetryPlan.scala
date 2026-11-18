/*
 * Copyright (c) 2026 Mark Grechanik and Lone Star Consulting, Inc.
 * All rights reserved.
 *
 * Project: aws-emulator-scala3-lab
 * File: RetryPlan.scala
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


/** Illustrative application retry policy; this does not inject emulator faults. */
object RetryPlan:
  enum Action:
    case RetryAfter(milliseconds: Long)
    case FailPermanently

  def choose(errorCode: String, retryIndex: Int): Action =
    val retryable = Set("ThrottlingException", "ServiceUnavailable", "RequestTimeout")
    if !retryable.contains(errorCode) || retryIndex >= 4 then
      Action.FailPermanently
    else
      val milliseconds = math.min(1600L, 100L * (1L << retryIndex))
      Action.RetryAfter(milliseconds)

  def main(args: Array[String]): Unit =
    for i <- 0 to 4 do
      println(s"throttling attempt=$i -> ${choose("ThrottlingException", i)}")
    println(s"validation -> ${choose("ValidationException", 0)}")
