#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 path/to/file.csv" >&2
  exit 2
fi

script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
project_root="$(cd -- "$script_dir/.." && pwd)"
csv_path="$(realpath "$1")"
leaf="$(basename "$csv_path")"
csv_path_lc="$(printf '%s' "$csv_path" | tr '[:upper:]' '[:lower:]')"

if [[ "$csv_path_lc" != *.csv ]]; then
  echo "Expected a CSV file: $csv_path" >&2
  exit 2
fi

cd "$project_root"
docker compose cp "$csv_path" "namenode:/tmp/$leaf"
docker compose exec -T namenode hdfs dfs -mkdir -p /user/cybershield/raw
docker compose exec -T namenode hdfs dfs -put -f "/tmp/$leaf" "/user/cybershield/raw/$leaf"
docker compose exec -T namenode hdfs dfs -ls /user/cybershield/raw
echo "Uploaded $leaf to /user/cybershield/raw/$leaf"
