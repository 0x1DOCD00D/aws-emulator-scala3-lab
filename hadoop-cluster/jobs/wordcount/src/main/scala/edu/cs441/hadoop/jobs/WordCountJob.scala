/*
 * Copyright (c) 2026 Mark Grechanik and Lone Star Consulting, Inc.
 * All rights reserved.
 *
 * Project: aws-emulator-scala3-lab
 * File: WordCountJob.scala
 * Author: Mark Grechanik
 * Date: 2026-11-19
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


package edu.cs441.hadoop.jobs

import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * Input  : one UTF-8 text line per mapper invocation, with its byte offset.
 * Output : one (lowercase Unicode word, 1) pair per token.
 * Example: "Cloud Hadoop Cloud" -> (cloud,1), (hadoop,1), (cloud,1).
 * Hadoop supplies each Context and partitions/interleaves mapper calls.
 */
class WordCountMapper extends Mapper[LongWritable, Text, Text, IntWritable]:
  override def map(key: LongWritable, value: Text, context: Context): Unit =
    val tokens = value.toString.toLowerCase(Locale.ROOT)
      .split("[^\\p{L}\\p{N}]+")
      .filter(_.nonEmpty)
    tokens.foreach(token => context.write(new Text(token), new IntWritable(1)))

/**
 * Input  : a word and iterable of IntWritable partial counts.
 * Output : the same word with its summed count.
 * This associative summation is safe as both combiner and reducer.
 */
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

/**
 * Execute inside actual Hadoop YARN, not inside the AWS Emulator EMR control plane.
 *
 * Usage:
 *   hadoop jar cs441-wordcount.jar edu.cs441.hadoop.jobs.WordCountJob \
 *     /user/hadoop/cs441/input/<run>/input.txt \
 *     /user/hadoop/cs441/output/<run>
 */
object WordCountJob:
  def main(args: Array[String]): Unit =
    if args.length != 2 then
      System.err.println("Usage: WordCountJob <HDFS-input> <HDFS-output>")
      System.exit(2)

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

    if !job.waitForCompletion(true) then
      System.err.println("MapReduce job failed; consult YARN logs at http://localhost:8088")
      System.exit(1)
