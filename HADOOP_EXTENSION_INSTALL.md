# Installing the additive Hadoop extension

This patch adds only NEW files to your working tree. It deliberately does not
modify `.gitignore`, `README.md`, `build.sbt`, `project/build.properties`, or
`project/plugins.sbt` because existing copies may differ between checkouts.

## Installation from the repository root

```bash
cd /Users/drmark/Github/aws-emulator-scala3-lab
# Use the actual location where you saved the downloaded patch.
git apply --check /path/to/hadoop-cluster-extension-fixed.patch
git apply /path/to/hadoop-cluster-extension-fixed.patch
```

The new `hadoop-cluster.sbt` is automatically loaded by sbt alongside your
existing `build.sbt`. The new file under `project/` loads sbt-assembly without
modifying your `project/plugins.sbt`. Your existing sbt launcher version stays
unchanged. The extension requires sbt 1.x and Java 21.

If your existing `project/plugins.sbt` already includes the **same** sbt-assembly
plugin, remove the duplicate from either file (not both). The default plugin
version for this extension is 2.3.1.

## Verify that the projects were discovered

```bash
sbt projects
sbt 'hadoopCluster/compile' 'hadoopJob/compile'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli self-test'
```

The project list should include `hadoopCluster` and `hadoopJob`. A successful
self-test does not require Docker or AWS Emulator.

## Start the Docker Hadoop cluster

```bash
hadoop-cluster/scripts/preflight.sh
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli setup'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli up'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli smoke'
```

Then run the Scala 3 Hadoop MapReduce job:

```bash
sbt 'hadoopJob/assembly'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli wordcount hadoop-cluster/data/wordcount.txt'
```

The controller accepts input paths relative to the repository root or the
Hadoop module, irrespective of the child JVM working directory.

To transfer input and results through local AWS Emulator S3, first source
`scripts/emulator-env.sh` in the shell and start AWS Emulator. Then follow the
`hadoop-cluster/README.md` instructions. The controller will refuse an AWS
account number other than `000000000000`.

## Why the previous patch failed

The earlier patch tried to add `.gitignore` and `project/build.properties`
as brand-new files, and to replace part of `build.sbt` using context that was
not present in your checkout. Git apply refuses those operations by design.
The corrected patch contains no modification hunks for existing paths.

If `git apply --check` reports an existing `hadoop-cluster/` file, inspect your
working tree with `git status --short` before deleting or overwriting anything.
Only run the corrected patch in a directory where the Hadoop module has not
already been installed. Do not use `git apply --reject` or force-overwrite
existing project files to get around a conflict.
