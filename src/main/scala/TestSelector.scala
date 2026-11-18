/*
 * Copyright (c) 2026 Mark Grechanik and Lone Star Consulting, Inc.
 * All rights reserved.
 *
 * Project: aws-emulator-scala3-lab
 * File: TestSelector.scala
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


/** An illustrative, finite optimization problem, not an AWS fidelity score. */
object TestSelector:
  final case class Check(name: String, minutes: Int,
                         impact: Double, detection: Double):
    def benefit: Double = impact * detection

  def main(args: Array[String]): Unit =
    val checks = Vector(
      Check("S3 round trip", 2, 5.0, 0.8),
      Check("DynamoDB conditional write", 3, 9.0, 0.5),
      Check("SQS receipt/visibility", 2, 6.0, 0.6)
    )
    val timeBudget = 4
    val feasible = (0 until (1 << checks.size)).map { bits =>
      checks.indices.collect {
        case i if (bits & (1 << i)) != 0 => checks(i)
      }.toVector
    }.filter(_.map(_.minutes).sum <= timeBudget)

    feasible.foreach { group =>
      val names = if group.isEmpty then "(none)"
                  else group.map(_.name).mkString(" + ")
      val value = group.map(_.benefit).sum
      println(f"$names%-44s benefit=$value%.1f")
    }
    val best = feasible.maxBy(_.map(_.benefit).sum)
    println(s"Best plan within $timeBudget minutes: " +
      best.map(_.name).mkString(" + "))
