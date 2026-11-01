#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "$script_dir/.." && pwd)"
csv_path="${1:-$project_root/data/sample/sample_traffic.csv}"

if [[ ! -f "$csv_path" ]]; then
  echo "CSV file not found: $csv_path" >&2
  exit 2
fi
csv_path="$(realpath "$csv_path")"
if [[ "${csv_path,,}" != *.csv ]]; then
  echo "Expected a CSV file: $csv_path" >&2
  exit 2
fi

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/cybershield-hadoop/hadoop-3.5.0}"
export HADOOP_CONF_DIR="$project_root/hadoop-conf"
export HDFS_DEFAULT_FS="${HDFS_DEFAULT_FS:-hdfs://localhost:9000}"
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-$(id -un)}"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH"

if [[ ! -x "$HADOOP_HOME/bin/hdfs" ]]; then
  echo "Hadoop was not found at $HADOOP_HOME. Install Hadoop 3.5.0 or set HADOOP_HOME." >&2
  exit 1
fi
if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
  echo "Java was not found at $JAVA_HOME. Install Java 17 or set JAVA_HOME." >&2
  exit 1
fi

python_venv="${CYBERSHIELD_PYTHON_VENV:-$HOME/cybershield-pyspark-venv}"
python_bin="$python_venv/bin/python"
if [[ ! -x "$python_bin" ]]; then
  python3 -m venv "$python_venv"
fi
if ! "$python_bin" -c 'import distutils.version, pandas, pymongo, pyspark, sklearn' >/dev/null 2>&1; then
  "$python_bin" -m pip install -r "$project_root/requirements.txt"
fi

name_dir="$HOME/cybershield-hdfs-data/name"
report="$(hdfs dfsadmin -report 2>/dev/null || true)"
if [[ "$report" != *"Live datanodes (1):"* ]]; then
  if ! hdfs dfsadmin -report >/dev/null 2>&1; then
    mkdir -p "$name_dir"
    if [[ ! -f "$name_dir/current/VERSION" ]]; then
      if find "$name_dir" -mindepth 1 -print -quit | grep -q .; then
        echo "NameNode directory is not empty but has no VERSION file: $name_dir" >&2
        echo "Inspect it before formatting HDFS." >&2
        exit 1
      fi
      hdfs namenode -format CyberShield
    fi
    hdfs --daemon start namenode
  fi

  report=""
  for _ in $(seq 1 40); do
    report="$(hdfs dfsadmin -report 2>/dev/null || true)"
    if [[ "$report" == *"Live datanodes (1):"* ]]; then
      break
    fi
    if [[ -z "$report" ]]; then
      sleep 1
      continue
    fi
    hdfs --daemon start datanode >/dev/null 2>&1 || true
    sleep 1
  done
fi
if [[ "$report" != *"Live datanodes (1):"* ]]; then
  echo "HDFS did not report one live DataNode. Check Hadoop logs under $HADOOP_HOME/logs." >&2
  exit 1
fi

hdfs dfs -mkdir -p /user/cybershield/raw
leaf="$(basename "$csv_path")"
input_uri="$HDFS_DEFAULT_FS/user/cybershield/raw/$leaf"
output_uri="$HDFS_DEFAULT_FS/user/cybershield/processed/latest"
hdfs dfs -put -f "$csv_path" "$input_uri"

mongo_args=(--skip-mongo)
if [[ -n "${MONGODB_URI:-}" ]]; then
  mongo_args=()
fi

cd "$project_root"
export PYSPARK_PYTHON="$python_bin"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-2}"
"$python_bin" -m cybershield.pipeline \
  --input "$input_uri" \
  --hdfs-output "$output_uri" \
  "${mongo_args[@]}"

echo "HDFS raw input: $input_uri"
echo "HDFS cleaned output: $output_uri"
echo "Local dashboard artifacts: $project_root/data/processed/"
