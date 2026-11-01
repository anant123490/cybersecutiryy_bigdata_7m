# CyberShield: Network Intrusion Detection and Cyber Threat Analytics Using Hadoop and Apache Spark

**College project report draft**

| Field | Project detail |
| --- | --- |
| Student name / roll number | Fill in |
| Department / institution | Fill in |
| Subject | Big Data Analytics |
| Guide | Fill in |
| Academic year | Fill in |
| Dataset used | Record the exact UNSW-NB15 or CIC-IDS2017 files used |
| Execution date | Fill in after a real run |

> Replace every bracketed result field with values from the student's own pipeline run. The repository ships a small synthetic CSV for demonstration; no benchmark score or execution result is claimed in this report.

## Abstract

CyberShield is an offline network-traffic analytics prototype for identifying flows labeled as normal or malicious in a public intrusion-detection dataset. It demonstrates a big-data workflow using Hadoop Distributed File System (HDFS), Apache Spark with PySpark, scikit-learn, MongoDB, Python, and Streamlit. The system accepts authorized labeled flow CSV files, stores them in HDFS, standardizes common UNSW-NB15 and CIC-IDS2017 field names, handles missing feature values, removes duplicate rows, and writes cleaned Parquet data back to HDFS. Spark computes traffic and protocol distributions, attack-category counts, unusual-volume indicators, hourly trends when timestamps are available, and source-IP counts among labeled attack rows. A bounded, approximately class-balanced sample is collected for a scikit-learn Random Forest classifier. A held-out split is evaluated using accuracy, macro precision, macro recall, macro F1, a per-class report, and a confusion matrix. MongoDB stores selected predictions and threat alerts, while a Streamlit dashboard visualizes the latest run. This is an educational batch-analysis prototype, not a live intrusion prevention system; conclusions depend on the chosen files, label quality, class imbalance, and evaluation split.

**Keywords:** Big Data Analytics, HDFS, PySpark, intrusion detection, Random Forest, MongoDB, Streamlit, network flows.

## 1. Introduction

Enterprise networks produce large volumes of connection and flow records. Manual inspection is difficult when each record contains multiple fields such as protocol, duration, ports, packet counts, byte counts, endpoint addresses, and a label. Big-data tools help organize storage and process many rows using parallel data operations. This project presents a reproducible offline workflow: HDFS stores the dataset, Spark performs scalable cleaning and aggregation, a standard machine-learning model classifies a bounded training sample, MongoDB keeps selected output records, and a dashboard supports review.

The input is a labeled research benchmark, not traffic captured from the student's network. Dataset IPs describe the dataset environment and do not establish that a current real-world host is malicious.

## 2. Problem statement

Given an offline labeled network-flow dataset, how can a student use a Hadoop-based storage and analytics pipeline to summarize traffic patterns, classify flow categories, surface candidate suspicious activity, and present measured results while making the storage path and evaluation limits visible?

## 3. Objectives

1. Store source network-flow CSV files in an HDFS namespace.
2. Use PySpark to read HDFS input, standardize fields, handle missing values and duplicates, and write cleaned Parquet output.
3. Compute flow counts, attack-category and protocol distributions, volume percentiles, timestamp trends, and source-IP counts associated with labeled attacks.
4. Train and evaluate a multiclass Random Forest on a bounded sample with a held-out split.
5. Report accuracy, precision, recall, F1-score, per-class scores, and a confusion matrix.
6. Assign transparent rule-based severity to predicted records.
7. Persist selected processed records, threat alerts, and run analytics in MongoDB.
8. Present the latest output in a Streamlit and Plotly dashboard.
9. Keep the workflow limited to authorized offline datasets and logs.

## 4. Existing and proposed systems

### 4.1 Existing approach

A small-scale manual or single-script workflow often loads a CSV into a spreadsheet or one Python process, filters suspicious rows, and plots a few charts. This can be useful for exploration but provides limited distributed-storage practice, is constrained by local memory for larger files, can hide preprocessing decisions, and may present aggregate accuracy without explaining class-level behavior.

### 4.2 Proposed approach

CyberShield makes the batch flow explicit. Input CSVs are uploaded to HDFS; Spark standardizes and analyzes the complete labeled data through DataFrame operations; cleaned data remains in HDFS as Parquet. Only a configurable bounded sample crosses to the Python driver for scikit-learn. MongoDB receives selected held-out predictions and attack alerts. The dashboard reads generated artifacts and labels all scores as outputs of the actual run.

## 5. System architecture

See [architecture.md](architecture.md) for the flow diagram. The main components are:

- **HDFS:** NameNode metadata plus DataNode block storage. The teaching deployment contains one NameNode and one DataNode, with replication factor one.
- **PySpark:** Runs in a container, reads the raw HDFS URI, performs cleaning and whole-dataset aggregations, and writes cleaned Parquet back to HDFS.
- **scikit-learn:** Fits a Random Forest pipeline on a sample bounded by a row limit; one-hot encodes categorical variables and imputes numeric values.
- **MongoDB:** Holds the CyberShield database and the traffic_processed, threat_alerts, and analytics_runs collections.
- **Streamlit and Plotly:** Provides charts, metrics, an alert table, and a confusion matrix from the most recent run.

### 5.1 Hadoop ecosystem explanation

Hadoop is a framework for distributed storage and processing. HDFS is its distributed file system. The NameNode records filesystem metadata and block locations; DataNodes hold the actual file blocks. Large files can be split into blocks and distributed across DataNodes. Replication helps recover from node failure in a cluster with multiple nodes. In this single-DataNode demonstration, replication is one, so it illustrates HDFS storage and commands but not fault tolerance.

Apache Spark is the analytics engine in this implementation. It reads HDFS files as Spark DataFrames, applies transformations and aggregations, and writes columnar Parquet. Spark executes in local mode on the laptop, using available local cores. Thus the project genuinely uses HDFS as storage but does not claim a multi-node Spark compute cluster.

## 6. Dataset description

### 6.1 Primary dataset

The default dataset is UNSW-NB15. Its official research page describes a 49-feature network dataset and provides CSV files, including a training partition of 175,341 records and testing partition of 82,332 records. The complete source dataset is described as 2,540,044 rows across four CSV files. Use the official [UNSW-NB15 dataset page](https://research.unsw.edu.au/projects/unsw-nb15-dataset) and download the labeled training and testing CSVs from its SharePoint link. Academic use is permitted subject to the dataset authors' citation requirements.

The attack-category values are Normal and nine attack families: Analysis, Backdoor, DoS, Exploits, Fuzzers, Generic, Reconnaissance, Shellcode, and Worms. The separate binary label is 0 for normal and 1 for attack. CyberShield uses attack_cat as the multiclass prediction target and uses the binary label to resolve blank category values for normal flows.

### 6.2 Optional dataset

The [CIC-IDS2017 official page](https://www.unb.ca/cic/datasets/ids-2017.html) provides labeled flow CSVs with endpoint addresses, ports, protocol, time, flow features, and labels. Its labels include benign and categories such as brute force, DoS/DDoS, web attack, infiltration, botnet, and Heartbleed. Upload selected labeled CSV files using the same HDFS path.

### 6.3 Fields used

| Canonical field | UNSW-NB15 examples | CIC-IDS2017 examples | Use |
| --- | --- | --- | --- |
| Source/destination IP | srcip, dstip | Source IP, Destination IP | Group attack-labeled rows and add alert context; excluded from model features |
| Source/destination port | sport, dsport | Source Port, Destination Port | Numeric classifier features |
| Protocol | proto | Protocol | Categorical classifier feature and protocol distribution |
| Service/state | service, state | Often not present | Categorical feature; missing values become unknown |
| Duration | dur | Flow Duration | Numeric classifier feature |
| Source/destination bytes | sbytes, dbytes | Fwd/Bwd packet-length totals | Numeric feature and volume analytics |
| Source/destination packets | spkts, dpkts | Total Fwd/Bwd Packets | Numeric feature and volume analytics |
| Timestamp | Stime | Timestamp | Hourly flow trend when parseable |
| Category | attack_cat | Label | Multiclass target |

The canonicalizer maps common column spelling variants. A feature missing from the chosen CSV is set to a safe default and should be disclosed when interpreting model performance.

### 6.4 Class distribution

Per-class counts vary by the files selected and should be calculated from the actual uploaded data. After execution, report the attack_distribution and normal_flows/labeled_attack_flows fields from data/processed/analytics.json and class_distribution_in_model_sample from model_metrics.json. The included synthetic 26-row file has Normal 12, DoS 4, Generic 4, Reconnaissance 3, and Exploits 3. These sample counts are for smoke demonstration only and are not benchmark statistics.

## 7. Methodology

### 7.1 Ingestion and HDFS storage

The PowerShell or Bash helper copies a local CSV into the NameNode container and uses the HDFS CLI to place it under /user/cybershield/raw. The Spark job reads the HDFS URI directly. The exact file list can be confirmed with hdfs dfs -ls. Raw dataset files are not committed to the repository.

### 7.2 Preprocessing

Spark reads CSV with its header. Headers are normalized by removing case and punctuation differences. Rows without a usable category are dropped; exact duplicate standardized rows are removed. Numeric traffic features are cast to doubles. Null and NaN values are imputed with a Spark-computed median; absent dataset features use zero. Missing service, state, protocol, and endpoint strings use unknown. Negative volume/duration values are floored to zero. Labels BENIGN, Normal, and equivalent values are normalized to Normal. The cleaned DataFrame is stored as Parquet in HDFS.

### 7.3 Big-data analytics

- **Attack frequency:** count flows grouped by attack_category.
- **Protocol mix:** count flows grouped by protocol.
- **Suspicious source IPs:** count flows grouped by src_ip where the supplied dataset label indicates an attack.
- **Unusual volume:** mark a flow if total bytes or packets meet or exceed that dataset's p99 threshold.
- **Traffic trend:** group parseable flow timestamps by hour and show total and attack-labeled counts.
- **Normal versus malicious:** compare Normal flows with non-Normal labeled categories.

These are dataset summaries. The suspicious-IP ranking is not an external reputation lookup or an indicator validation.

### 7.4 Machine learning

The model is a scikit-learn RandomForestClassifier with 200 trees and balanced-sub-sample class weights. It predicts the attack category as a multiclass label. Numeric features are ports, duration, bytes, and packet counts. Protocol, service, and state use one-hot encoding. Endpoint IP addresses are excluded to reduce host memorization.

Spark takes an approximately class-balanced sample with a default upper limit of 100,000 rows. The driver applies a reproducible 75/25 train/test split, stratified when available class sizes allow. The sampled set is bounded, but full dataset cleaning and analytics still read all input rows. The sample cap and random seed can be changed from the CLI.

Let TP, TN, FP, and FN denote counts for a class in a one-versus-rest view:

- Accuracy = (TP + TN) / (TP + TN + FP + FN)
- Precision = TP / (TP + FP)
- Recall = TP / (TP + FN)
- F1 = 2 × precision × recall / (precision + recall)

CyberShield presents macro-averaged precision, recall, and F1 so each class contributes equally to the average. The confusion matrix provides per-class context. Accuracy alone may appear strong when the class distribution is imbalanced; review minority-class recall and precision.

### 7.5 Severity rules

Severity is a transparent triage label assigned to the predicted category:

| Severity | Rule |
| --- | --- |
| Critical | Predicted category name contains DoS, DDoS, Worm, Shellcode, Heartbleed, or Bot |
| High | Other predicted attack whose total bytes or packets are at or above the dataset's p95 |
| Medium | Other predicted attack |
| Low | Predicted category is Normal |

The rule is a simple academic heuristic, not a validated incident severity scale. It does not block or respond to traffic.

### 7.6 Persistence and visualization

MongoDB stores held-out predictions in traffic_processed, attack predictions in threat_alerts, and the analytics summary in analytics_runs. The dashboard renders run totals, category and protocol charts, hourly trend, top source IP counts, metrics, a confusion matrix, and the held-out suspicious-prediction table.

## 8. Implementation and execution

The full setup and command sequence are in the [README](../README.md). In summary:

1. Start the compose services with Docker.
2. Upload the included synthetic sample or public labeled CSVs to HDFS.
3. Run python -m cybershield.pipeline in the app container.
4. Inspect HDFS with hdfs dfs -ls -R /user/cybershield.
5. Open the dashboard at http://localhost:8501 and MongoDB on port 27017.

Key outputs are analytics.json, model_metrics.json, predictions.csv, cleaned Parquet in HDFS, and MongoDB collections. The Docker environment makes Python, Java, Spark, HDFS networking, and MongoDB consistent across Windows/WSL and Linux.

## 9. Results and discussion

### 9.1 Results to fill after executing the project

| Measure | Actual value from run |
| --- | --- |
| Dataset file names and source | [Fill in] |
| Raw rows read | [analytics.json: raw_rows_read] |
| Flows after cleaning | [analytics.json: flows_after_cleaning] |
| Exact raw duplicates removed | [analytics.json: duplicate_raw_rows_removed] |
| Missing values before imputation | [analytics.json: missing_values_before_imputation] |
| Normal / attack-labeled flows | [normal_flows] / [labeled_attack_flows] |
| Most frequent attack category | [Read attack_distribution] |
| Most frequent protocol | [Read protocol_distribution] |
| Model sample / train / held-out counts | [model_metrics.json] |
| Accuracy / macro precision / macro recall / macro F1 | [model_metrics.json] |
| HDFS Parquet output path | [Record path and hdfs dfs -ls evidence] |
| MongoDB processed rows / alerts stored | [Record run output or database counts] |

No values are prefilled because the project has not been executed against a dataset in preparing this report. The dashboard and JSON output should be used as the evidence source. A single run on the synthetic sample is only a workflow demonstration and is not a meaningful performance estimate.

### 9.2 Discussion guide

Discuss whether the most frequent classes align with the selected dataset files; whether classes with low support have weaker recall; how one-hot encoding and class weighting affect learning; how cleaning changed record counts; and whether volume-based severity highlights the largest flows. Compare macro scores with accuracy and use the confusion matrix to describe likely confusions. Avoid claiming general real-world detection performance from one benchmark split.

## 10. Advantages

- Demonstrates actual HDFS commands, HDFS-backed input, and HDFS Parquet output.
- Performs whole-dataset aggregation with Spark DataFrames.
- Bounds driver-side scikit-learn memory use.
- Produces reproducible metrics and visible per-class results.
- Persists reviewable records in MongoDB and provides a dashboard.
- Supports both a synthetic sample and two public offline dataset formats.

## 11. Limitations

- A single NameNode/DataNode and local-mode Spark are teaching infrastructure, not production scale or fault tolerance.
- The model is evaluated on a bounded random sample; findings may differ from full or time-separated evaluation.
- The main example combines uploaded training/testing CSVs and makes a new random split rather than preserving the benchmark's official split.
- Public benchmark data may contain outdated traffic patterns, class imbalance, labeling artifacts, and dataset-specific features.
- Missing fields defaulted to zero/unknown can reduce model quality and should be reported.
- Severity thresholds are heuristic and relative to the selected dataset.
- IP rankings are based on the dataset's own labels; there is no live reputation enrichment.
- The batch pipeline does not parse PCAP files, monitor a live interface, authenticate users, or trigger defensive response.

## 12. Future enhancements

1. Deploy Spark on YARN or Kubernetes with multiple workers and an HDFS cluster with multiple DataNodes.
2. Keep the official training and test partitions separate and add time-based or cross-dataset evaluation.
3. Add configurable schema validation and missing-value summaries.
4. Evaluate Decision Tree and Logistic Regression baselines alongside the Random Forest.
5. Add probability calibration, threshold selection, and model/version tracking.
6. Add role-based access, MongoDB authentication, audit logging, and protected secrets for shared deployments.
7. Support new labeled datasets through explicit schema plugins; keep PCAP feature extraction as a separate offline component.
8. Add drift monitoring and analyst review workflows before considering any production use.

## 13. Conclusion

CyberShield demonstrates an end-to-end educational big-data workflow for offline intrusion analytics. HDFS supplies distributed filesystem storage, Spark processes cleaned traffic and computes summaries, scikit-learn produces measurable multiclass predictions, MongoDB stores selected results, and Streamlit presents them for review. Its design makes the sample cap, evaluation split, class distribution, severity heuristic, and single-machine execution limits explicit. Results should be reported only after running the project on clearly identified labeled files.

## References

1. Moustafa, N., and Slay, J. “UNSW-NB15: a comprehensive data set for network intrusion detection systems (UNSW-NB15 network data set).” Military Communications and Information Systems Conference (MilCIS), 2015. Dataset page: https://research.unsw.edu.au/projects/unsw-nb15-dataset
2. Moustafa, N., and Slay, J. “The evaluation of Network Anomaly Detection Systems: Statistical analysis of the UNSW-NB15 dataset and the comparison with the KDD99 dataset.” Information Security Journal: A Global Perspective, 2016. Citation details are listed on the UNSW-NB15 page.
3. Sharafaldin, I., Lashkari, A. H., and Ghorbani, A. A. “Toward Generating a New Intrusion Detection Dataset and Intrusion Traffic Characterization.” ICISSP, 2018. CIC-IDS2017 page: https://www.unb.ca/cic/datasets/ids-2017.html
4. Apache Software Foundation. Hadoop single-node cluster and HDFS documentation: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html
5. Apache Software Foundation. Apache Spark documentation: https://spark.apache.org/docs/latest/
6. Scikit-learn developers. Scikit-learn documentation: https://scikit-learn.org/stable/
7. MongoDB, Inc. MongoDB manual: https://www.mongodb.com/docs/manual/
8. Streamlit, Inc. Streamlit documentation: https://docs.streamlit.io/
