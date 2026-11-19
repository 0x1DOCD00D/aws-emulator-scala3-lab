#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
"$ROOT/hadoop-cluster/scripts/preflight.sh"
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli setup'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli up'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli smoke'
sbt 'hadoopJob/assembly'
sbt 'hadoopCluster/runMain edu.cs441.hadoop.cluster.HadoopClusterCli wordcount hadoop-cluster/data/wordcount.txt'
