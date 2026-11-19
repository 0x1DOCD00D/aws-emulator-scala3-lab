package edu.cs441.hadoop.cluster

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

/** Execute a fixed argument vector directly (no shell interpretation or interpolation). */
object CommandRunner:
  final case class Result(command: Seq[String], exitCode: Int, output: String):
    def checked: Result =
      if exitCode != 0 then
        throw new IllegalStateException(
          s"Command failed (exit=$exitCode): ${command.mkString(" ")}\n$output"
        )
      this

  def run(
      command: Seq[String],
      cwd: Path,
      timeoutSeconds: Long = 120L,
      failOnNonzero: Boolean = true,
      showOutput: Boolean = false
  ): Result =
    require(command.nonEmpty, "Command vector cannot be empty")
    val pb = new ProcessBuilder(command.asJava)
      .directory(cwd.toAbsolutePath.normalize().toFile)
      .redirectErrorStream(true)
    val child = pb.start()
    val output = new ByteArrayOutputStream()
    val maximumBytes = 2 * 1024 * 1024
    val collector = new Thread(
      new Runnable {
        override def run(): Unit = {
          val input = child.getInputStream
          val chunk = new Array[Byte](8192)
          try {
            var read = input.read(chunk)
            while (read >= 0) {
              val remaining = maximumBytes - output.size()
              if (remaining > 0 && read > 0)
                output.write(chunk, 0, math.min(read, remaining))
              read = input.read(chunk)
            }
          } finally input.close()
        }
      },
      "cs441-process-output"
    )
    collector.setDaemon(true)
    collector.start()

    val completed = child.waitFor(timeoutSeconds, TimeUnit.SECONDS)
    if !completed then
      child.destroyForcibly()
      child.waitFor(10, TimeUnit.SECONDS)
    collector.join(2000)
    val captured = new String(output.toByteArray, StandardCharsets.UTF_8) +
      (if output.size() == maximumBytes then "\n[output truncated at 2 MiB]" else "")
    if !completed then
      throw new IllegalStateException(
        s"Command timed out after $timeoutSeconds seconds: ${command.mkString(" ")}\n$captured"
      )
    val result = Result(command, child.exitValue(), captured)
    if showOutput && captured.nonEmpty then print(captured)
    if failOnNonzero then result.checked else result
