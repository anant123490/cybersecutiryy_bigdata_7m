"""Read offline IDS CSVs from HDFS, analyze them with Spark, and train a bounded sklearn model."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pymongo import MongoClient, ReplaceOne
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, precision_recall_fscore_support
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

LOGGER = logging.getLogger("cybershield")
NUMERIC_FEATURES = [
    "src_port", "dst_port", "duration", "src_bytes", "dst_bytes", "src_packets", "dst_packets",
]
CATEGORICAL_FEATURES = ["protocol", "service", "state"]
MODEL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
IP_FIELDS = ["src_ip", "dst_ip"]
CRITICAL_CATEGORIES = ("dos", "ddos", "worm", "shellcode", "heartbleed", "bot")

COLUMN_ALIASES = {
    "src_ip": {"srcip", "sourceip", "sourceaddress"},
    "dst_ip": {"dstip", "destinationip", "destinationaddress"},
    "src_port": {"sport", "sourceport"},
    "dst_port": {"dsport", "destinationport"},
    "protocol": {"proto", "protocol"},
    "service": {"service"},
    "state": {"state"},
    "duration": {"dur", "flowduration"},
    "src_bytes": {
        "sbytes", "totalfwdbytes", "totlenfwdpkts", "totallengthoffwdpackets", "totalfwdpacketlength",
    },
    "dst_bytes": {
        "dbytes", "totalbwdbytes", "totlenbwdpkts", "totallengthofbwdpackets", "totalbwdpacketlength",
    },
    "src_packets": {"spkts", "totalfwdpackets", "totalfwdpkts"},
    "dst_packets": {"dpkts", "totalbackwardpackets", "totalbwdpkts"},
    "event_time": {"stime", "timestamp", "flowstarttime", "datetime", "date"},
    "attack_cat": {"attackcat", "attackcategory"},
    "binary_label": {"label", "binarylabel"},
}


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def find_column(columns: list[str], aliases: set[str]) -> str | None:
    normalized = {normalize_header(column): column for column in columns}
    return next((normalized[alias] for alias in sorted(aliases) if alias in normalized), None)


def build_standardized_frame(
    raw: DataFrame,
) -> tuple[DataFrame, dict[str, str | None], dict[str, dict[str, int]]]:
    columns = raw.columns
    sources = {key: find_column(columns, aliases) for key, aliases in COLUMN_ALIASES.items()}

    # UNSW-NB15 has attack_cat plus a binary label; CIC-IDS2017 uses Label as the category.
    category_source = sources["attack_cat"] or find_column(columns, {"attackcat", "attackcategory", "label"})
    if not category_source:
        raise ValueError("No label column found. Supply a labeled IDS CSV (attack_cat or Label).")

    def as_string(source_key: str, default: str = "unknown"):
        source = sources[source_key]
        return F.trim(F.col(source).cast("string")) if source else F.lit(default)

    def as_number(source_key: str):
        source = sources[source_key]
        return F.col(source).cast(DoubleType()) if source else F.lit(None).cast(DoubleType())

    raw_category = F.trim(F.col(category_source).cast("string"))
    raw_category_lower = F.lower(raw_category)
    if sources["attack_cat"] == category_source and sources["binary_label"]:
        binary = F.col(sources["binary_label"]).cast("string")
        raw_category = F.when(
            raw_category.isNull() | (raw_category == "")
            | raw_category_lower.isin("nan", "null", "none"),
            F.when(binary.isin("0", "0.0", "false"), F.lit("Normal")).otherwise(F.lit("Unknown")),
        ).otherwise(raw_category)
    category_lower = F.lower(raw_category)
    category = F.when(
        category_lower.isin("benign", "normal", "background", "0", "0.0"), F.lit("Normal")
    ).otherwise(raw_category)

    expressions = [
        as_string("src_ip").alias("src_ip"),
        as_string("dst_ip").alias("dst_ip"),
        as_string("protocol").alias("protocol"),
        as_string("service").alias("service"),
        as_string("state").alias("state"),
        category.alias("attack_category"),
        as_string("event_time", "").alias("_event_time_raw"),
    ]
    expressions += [as_number(name).alias(name) for name in NUMERIC_FEATURES]
    frame = raw.select(*expressions)
    frame = frame.filter(
        F.col("attack_category").isNotNull()
        & (F.trim(F.col("attack_category")) != "")
        & (~F.lower(F.col("attack_category")).isin("nan", "null", "none"))
    ).dropDuplicates()

    missing_numeric_row = frame.agg(*[
        F.sum(F.when(F.col(name).isNull() | F.isnan(F.col(name)), 1).otherwise(0))
        .cast("long").alias(name)
        for name in NUMERIC_FEATURES
    ]).first()
    missing_string_row = frame.agg(*[
        F.sum(F.when(
            F.col(name).isNull() | (F.trim(F.col(name)) == "")
            | F.lower(F.trim(F.col(name))).isin("nan", "null", "none"),
            1,
        ).otherwise(0)).cast("long").alias(name)
        for name in CATEGORICAL_FEATURES + IP_FIELDS
    ]).first()
    missing_summary = {
        "numeric": {name: int(missing_numeric_row[name] or 0) for name in NUMERIC_FEATURES},
        "string": {
            name: int(missing_string_row[name] or 0)
            for name in CATEGORICAL_FEATURES + IP_FIELDS
        },
    }

    # Numeric nulls/NaNs are imputed with a Spark-computed median; absent features default to zero.
    for name in NUMERIC_FEATURES:
        frame = frame.withColumn(name, F.when(F.isnan(F.col(name)), None).otherwise(F.col(name)))
        quantiles = frame.approxQuantile(name, [0.5], 0.01) if sources.get(name) else []
        median = float(quantiles[0]) if quantiles else 0.0
        frame = frame.withColumn(name, F.greatest(F.coalesce(F.col(name), F.lit(median)), F.lit(0.0)))

    for name in CATEGORICAL_FEATURES + IP_FIELDS:
        frame = frame.withColumn(
            name,
            F.when(
                F.col(name).isNull() | (F.trim(F.col(name)) == "")
                | F.lower(F.trim(F.col(name))).isin("nan", "null", "none"),
                F.lit("unknown"),
            )
            .otherwise(F.col(name)),
        )

    raw_time = F.col("_event_time_raw")
    parsed_string_time = F.coalesce(*[
        F.try_to_timestamp(raw_time, F.lit(pattern))
        for pattern in (
            "M/d/yyyy H:mm",
            "M/d/yyyy H:mm:ss",
            "M/d/yyyy h:mm:ss a",
            "MM/dd/yyyy HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss",
        )
    ])
    frame = frame.withColumn(
        "event_timestamp",
        F.when(
            raw_time.rlike(r"^\d{13}(\.\d+)?$"),
            F.to_timestamp(F.from_unixtime((raw_time.cast("double") / 1000).cast("long"))),
        ).when(
            raw_time.rlike(r"^\d{10}(\.\d+)?$"),
            F.to_timestamp(F.from_unixtime(raw_time.cast("double").cast("long"))),
        ).otherwise(parsed_string_time),
    )
    frame = frame.withColumn("bytes_total", F.col("src_bytes") + F.col("dst_bytes"))
    frame = frame.withColumn("packets_total", F.col("src_packets") + F.col("dst_packets"))
    frame = frame.withColumn("is_attack", F.col("attack_category") != F.lit("Normal"))
    return frame, sources, missing_summary


def approximate_thresholds(frame: DataFrame) -> dict[str, float]:
    thresholds: dict[str, float] = {}
    for column in ("bytes_total", "packets_total"):
        values = frame.approxQuantile(column, [0.95, 0.99], 0.01)
        thresholds[f"{column}_p95"] = float(values[0]) if values else 0.0
        thresholds[f"{column}_p99"] = float(values[1]) if values else 0.0
    return thresholds


def collect_pairs(frame: DataFrame, value: str = "count", limit: int = 50) -> list[dict[str, Any]]:
    return [row.asDict() for row in frame.orderBy(F.desc(value)).limit(limit).collect()]


def calculate_analytics(frame: DataFrame, thresholds: dict[str, float], raw_row_count: int) -> dict[str, Any]:
    total = frame.count()
    attack_count = frame.filter(F.col("is_attack")).count()
    normal_count = total - attack_count
    attack_categories = collect_pairs(
        frame.filter(F.col("is_attack")).groupBy("attack_category").count(), limit=50
    )
    protocol_distribution = collect_pairs(frame.groupBy("protocol").count(), limit=50)
    top_ips = collect_pairs(
        frame.filter(F.col("is_attack") & (F.col("src_ip") != "unknown"))
        .groupBy("src_ip").count(),
        limit=20,
    )
    unusual = frame.filter(
        (F.col("bytes_total") >= F.lit(thresholds["bytes_total_p99"]))
        | (F.col("packets_total") >= F.lit(thresholds["packets_total_p99"]))
    ).count()
    trend_frame = frame.filter(F.col("event_timestamp").isNotNull()).withColumn(
        "hour", F.date_trunc("hour", F.col("event_timestamp"))
    )
    trend = [
        {
            "hour": row["hour"].isoformat() if row["hour"] else None,
            "count": row["count"],
            "attack_count": row["attack_count"],
        }
        for row in trend_frame.groupBy("hour")
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.sum(F.col("is_attack").cast("int")).alias("attack_count"),
        )
        .orderBy("hour").limit(1000).collect()
    ]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "raw_rows_read": int(raw_row_count),
        "flows_after_cleaning": int(total),
        "normal_flows": int(normal_count),
        "labeled_attack_flows": int(attack_count),
        "unusual_volume_flows": int(unusual),
        "attack_distribution": attack_categories,
        "protocol_distribution": protocol_distribution,
        "top_suspicious_source_ips": top_ips,
        "traffic_trend": trend,
        "traffic_trend_has_timestamps": bool(trend),
        "volume_thresholds": thresholds,
    }


def severity_for(predicted_category: str, total_bytes: float, total_packets: float,
                 thresholds: dict[str, float]) -> str:
    if predicted_category == "Normal":
        return "low"
    if any(token in predicted_category.lower() for token in CRITICAL_CATEGORIES):
        return "critical"
    if (total_bytes >= thresholds.get("bytes_total_p95", math.inf)
            or total_packets >= thresholds.get("packets_total_p95", math.inf)):
        return "high"
    return "medium"


def train_and_predict(frame: DataFrame, max_sample: int, thresholds: dict[str, float],
                      random_state: int) -> tuple[dict[str, Any], pd.DataFrame]:
    counts = {
        row["attack_category"]: int(row["count"])
        for row in frame.groupBy("attack_category").count().collect()
    }
    per_class = max(2, math.ceil(max_sample / max(1, len(counts))))
    fractions = {label: min(1.0, per_class / count) for label, count in counts.items() if count > 0}
    sample = frame.sampleBy("attack_category", fractions, seed=random_state).limit(max_sample)
    columns = IP_FIELDS + MODEL_FEATURES + [
        "attack_category", "bytes_total", "packets_total", "event_timestamp",
    ]
    pdf = sample.select(*columns).toPandas()
    if pdf.empty:
        raise ValueError("No labeled rows remain after cleaning; cannot train a model.")
    pdf["attack_category"] = pdf["attack_category"].astype(str)
    if pdf["attack_category"].nunique() < 2:
        raise ValueError("At least two traffic classes are required to train and evaluate the model.")

    X = pdf[MODEL_FEATURES].copy()
    for column in NUMERIC_FEATURES:
        X[column] = pd.to_numeric(X[column], errors="coerce")
    for column in CATEGORICAL_FEATURES:
        X[column] = X[column].fillna("unknown").astype(str)
    y = pdf["attack_category"]
    class_counts = y.value_counts()
    can_stratify = class_counts.min() >= 2 and int(len(pdf) * 0.25) >= y.nunique()
    X_train, X_test, y_train, y_test, rows_train, rows_test = train_test_split(
        X, y, pdf, test_size=0.25, random_state=random_state,
        stratify=y if can_stratify else None,
    )

    numeric_pipe = Pipeline([("imputer", SimpleImputer(strategy="median"))])
    categorical_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
        ("onehot", OneHotEncoder(handle_unknown="ignore")),
    ])
    preprocess = ColumnTransformer([
        ("numeric", numeric_pipe, NUMERIC_FEATURES),
        ("categorical", categorical_pipe, CATEGORICAL_FEATURES),
    ])
    model = Pipeline([
        ("preprocess", preprocess),
        ("classifier", RandomForestClassifier(
            n_estimators=200, class_weight="balanced_subsample",
            random_state=random_state, n_jobs=-1,
        )),
    ])
    model.fit(X_train, y_train)
    predicted = model.predict(X_test)
    labels = sorted(y.unique().tolist())
    accuracy = float(accuracy_score(y_test, predicted))
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test, predicted, average="macro", zero_division=0,
    )
    metrics = {
        "model": "RandomForestClassifier",
        "task": "multiclass flow-category classification",
        "training_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "sample_rows": int(len(pdf)),
        "max_model_sample_rows": int(max_sample),
        "stratified_split": bool(can_stratify),
        "random_state": random_state,
        "class_distribution_in_model_sample": {str(k): int(v) for k, v in class_counts.items()},
        "labels": labels,
        "accuracy": accuracy,
        "precision_macro": float(precision),
        "recall_macro": float(recall),
        "f1_macro": float(f1),
        "classification_report": classification_report(
            y_test, predicted, labels=labels, output_dict=True, zero_division=0,
        ),
        "confusion_matrix": confusion_matrix(y_test, predicted, labels=labels).tolist(),
        "threshold_note": "Metrics are from the held-out test split of the bounded sklearn sample.",
    }
    prediction_rows = rows_test.copy()
    prediction_rows["actual_category"] = y_test.to_numpy()
    prediction_rows["predicted_category"] = predicted
    prediction_rows["predicted_is_attack"] = prediction_rows["predicted_category"] != "Normal"
    prediction_rows["severity"] = prediction_rows.apply(
        lambda row: severity_for(
            str(row["predicted_category"]), float(row["bytes_total"]),
            float(row["packets_total"]), thresholds,
        ),
        axis=1,
    )
    prediction_rows["prediction_correct"] = (
        prediction_rows["actual_category"] == prediction_rows["predicted_category"]
    )
    prediction_rows["event_timestamp"] = prediction_rows["event_timestamp"].astype(str)
    return metrics, prediction_rows


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if hasattr(value, "item"):
        return value.item()
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def row_document(row: dict[str, Any]) -> dict[str, Any]:
    document = json_safe(row)
    stable = json.dumps(document, sort_keys=True, separators=(",", ":"), default=str)
    document["_id"] = hashlib.sha256(stable.encode("utf-8")).hexdigest()
    document["stored_at_utc"] = datetime.now(timezone.utc).isoformat()
    return document


def persist_to_mongodb(metrics: dict[str, Any], predictions: pd.DataFrame, uri: str,
                       db_name: str, max_docs: int) -> dict[str, int]:
    documents = [row_document(row) for row in predictions.head(max_docs).to_dict(orient="records")]
    processed = [ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) for doc in documents]
    alert_docs = [doc for doc in documents if doc.get("predicted_is_attack")]
    alerts = [ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) for doc in alert_docs]
    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    try:
        client.admin.command("ping")
        database = client[db_name]
        for collection_name in ("traffic_processed", "threat_alerts", "analytics_runs"):
            if collection_name not in database.list_collection_names():
                database.create_collection(collection_name)
        if processed:
            database.traffic_processed.bulk_write(processed, ordered=False)
        if alerts:
            database.threat_alerts.bulk_write(alerts, ordered=False)
        database.analytics_runs.update_one(
            {"generated_at_utc": metrics["generated_at_utc"]},
            {"$set": json_safe(metrics)},
            upsert=True,
        )
        database.threat_alerts.create_index([("severity", 1), ("predicted_category", 1)])
        database.threat_alerts.create_index([("src_ip", 1)])
        return {
            "processed_records_upserted": len(processed),
            "threat_alerts_upserted": len(alerts),
        }
    finally:
        client.close()


def write_json(path: Path, content: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe(content), indent=2, ensure_ascii=False), encoding="utf-8")


def write_local_csv(frame: DataFrame, path: Path) -> None:
    """Stream a cleaned DataFrame to CSV with Python I/O for host-only Windows runs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = frame.columns
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        for row in frame.toLocalIterator():
            writer.writerow({
                key: (value.isoformat() if hasattr(value, "isoformat") else value)
                if value is not None else ""
                for key, value in row.asDict().items()
            })


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=os.getenv("HDFS_INPUT", "data/sample/sample_traffic.csv"),
    )
    parser.add_argument(
        "--hdfs-output",
        default=os.getenv("HDFS_OUTPUT"),
    )
    parser.add_argument("--local-output", default=os.getenv("LOCAL_OUTPUT_DIR", "data/processed"))
    parser.add_argument("--max-model-sample", type=int, default=100_000)
    parser.add_argument("--max-mongo-docs", type=int, default=20_000)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--skip-mongo", action="store_true", help="Run without writing MongoDB records.")
    args = parser.parse_args()
    if args.max_model_sample < 8:
        parser.error("--max-model-sample must be at least 8")

    local_mode = not args.input.startswith("hdfs://")
    if local_mode and not args.input.startswith("file:"):
        args.input = Path(args.input).resolve().as_uri()
    output_uri = args.hdfs_output
    if not output_uri:
        output_uri = (
            str(Path(args.local_output).resolve() / "cleaned_traffic.csv")
            if local_mode
            else "hdfs://namenode:9000/user/cybershield/processed/latest"
        )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = (
        SparkSession.builder.appName("CyberShield-Offline-IDS-Analytics")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.hadoop.fs.defaultFS", "file:///" if local_mode else "hdfs://namenode:9000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        LOGGER.info("Reading labeled CSV data from %s", args.input)
        raw = (
            spark.read.option("header", True).option("mode", "PERMISSIVE")
            .option("inferSchema", False).csv(args.input)
        )
        raw_count = raw.count()
        distinct_raw_count = raw.dropDuplicates().count()
        frame, detected_sources, missing_summary = build_standardized_frame(raw)
        frame = frame.cache()
        clean_count = frame.count()
        if clean_count == 0:
            raise ValueError("No labeled traffic rows remained after cleaning.")
        thresholds = approximate_thresholds(frame)
        analytics = calculate_analytics(frame, thresholds, raw_count)
        analytics["execution_mode"] = "local Spark; local files" if local_mode else "Spark reading HDFS"
        analytics["cleaned_data_output"] = output_uri
        analytics["hdfs_used"] = not local_mode
        analytics["duplicate_raw_rows_removed"] = int(raw_count - distinct_raw_count)
        analytics["detected_input_columns"] = detected_sources
        analytics["missing_values_before_imputation"] = missing_summary

        if local_mode:
            LOGGER.info("Writing cleaned Spark rows to local CSV: %s", output_uri)
            write_local_csv(frame, Path(output_uri))
        else:
            LOGGER.info("Writing cleaned Spark DataFrame to HDFS: %s", output_uri)
            frame.write.mode("overwrite").parquet(output_uri)
        model_metrics, prediction_rows = train_and_predict(
            frame, args.max_model_sample, thresholds, args.random_state,
        )
        model_metrics["severity_rules"] = {
            "critical": "Predicted attack category contains DoS/DDoS/Worm/Shellcode/Heartbleed/Bot.",
            "high": "Other predicted attack with total bytes or packets at or above the dataset p95.",
            "medium": "Other predicted attack.",
            "low": "Predicted Normal.",
            "volume_thresholds": thresholds,
        }
        analytics["model_metrics"] = model_metrics
        output_dir = Path(args.local_output)
        output_dir.mkdir(parents=True, exist_ok=True)
        write_json(output_dir / "analytics.json", analytics)
        write_json(output_dir / "model_metrics.json", model_metrics)
        prediction_rows.to_csv(output_dir / "predictions.csv", index=False)
        if not args.skip_mongo and not local_mode:
            mongo_counts = persist_to_mongodb(
                analytics, prediction_rows, os.getenv("MONGODB_URI", "mongodb://mongodb:27017"),
                os.getenv("MONGODB_DATABASE", "CyberShield"), args.max_mongo_docs,
            )
            analytics["mongodb_write"] = mongo_counts
            write_json(output_dir / "analytics.json", analytics)
        elif local_mode:
            analytics["mongodb_write"] = {"status": "skipped", "reason": "local-only mode"}
            write_json(output_dir / "analytics.json", analytics)
        LOGGER.info(
            "Complete: %s raw rows, %s cleaned flows, %s held-out predictions. Metrics: %s",
            raw_count, clean_count, model_metrics["test_rows"], output_dir / "model_metrics.json",
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
