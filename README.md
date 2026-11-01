# CyberShield

**Network Intrusion Detection and Cyber Threat Analytics Using Hadoop and Apache Spark**

CyberShield is a defensive, offline analytics project for labeled network-flow CSV files. Hadoop HDFS stores source data and cleaned output; PySpark computes traffic analytics; scikit-learn trains and evaluates a multiclass model on a bounded sample; Streamlit presents the latest run. MongoDB is an optional sink for predictions and alerts. This implementation uses Spark for processing; Pig and Hive are not required.

Only use public benchmark files or network logs you are authorized to analyze. This project does not capture traffic, scan networks, or take response actions.

## Architecture

~~~text
offline flow CSV ──upload──> HDFS raw/
                                  │
                                  ▼
                    PySpark cleaning + analytics
                       │                  │
                       ▼                  ▼
             HDFS cleaned Parquet    bounded sample
                                           │
                                  sklearn Random Forest
                                     │             │
                                     ▼             ▼
                              MongoDB alerts   JSON/CSV
                                                    │
                                             Streamlit dashboard
~~~

The native WSL runner and optional compose setup use a real HDFS NameNode and DataNode, and Spark reads its input from HDFS. For laptop accessibility, Spark compute runs in local mode on one machine and HDFS has one DataNode with replication set to one. It demonstrates distributed storage and the Spark/HDFS data path; it is not a fault-tolerant production cluster or multi-worker Spark cluster.

## Folder structure

~~~text
CyberShield/
├── cybershield/pipeline.py       # HDFS → Spark analytics → sklearn → MongoDB
├── dashboard/app.py              # Streamlit/Plotly dashboard
├── data/
│   ├── sample/sample_traffic.csv  # 26-row synthetic demonstration CSV
│   ├── raw/                       # put downloaded CSVs here before upload
│   └── processed/                 # latest run JSON/CSV artifacts
├── docs/
│   ├── architecture.md
│   ├── academic_report.md
│   ├── presentation_and_viva.md
│   └── troubleshooting.md
├── scripts/run_hadoop_spark.ps1
├── scripts/run_hadoop_spark_wsl.sh
├── scripts/upload_to_hdfs.ps1
├── scripts/upload_to_hdfs.sh
├── hadoop-conf/                  # native single-node Hadoop configuration
├── docker-compose.yml
├── Dockerfile
├── hadoop.env
└── requirements.txt
~~~

## Run locally with normal Python (no Docker)

This host-only mode uses the included synthetic sample, Spark local mode, and local files. It does not use HDFS or MongoDB. It is useful for running the analytics and dashboard on a machine without Docker. The full HDFS/Mongo deployment instructions follow below.

Requirements: Python 3.11 or 3.12 and a supported JDK on PATH. The commands below use the installed JDK 21 and the project's virtual environment. In PowerShell:

~~~powershell
python -m venv --system-site-packages .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-21'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
.\.venv\Scripts\python.exe -m cybershield.pipeline
~~~

On Linux or WSL, create and activate the virtual environment, install the same requirements, set JAVA_HOME to your JDK, and run:

~~~shell
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m cybershield.pipeline
~~~

The local run reads data/sample/sample_traffic.csv, writes a cleaned CSV plus JSON/CSV analytics artifacts under data/processed/, and skips MongoDB. The cleaned CSV path avoids Hadoop's Windows-only winutils.exe requirement; the HDFS deployment writes Parquet. Start the dashboard in a second terminal:

~~~powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py
~~~

Then open http://localhost:8501. To use a local labeled CSV, pass --input path/to/file.csv. To analyze another local sample, the same command applies.

## Native Hadoop + Spark on WSL (no Docker)

This is the full HDFS and Spark path for Windows. It uses Ubuntu 24.04 in WSL, Java 17, Hadoop 3.5.0, and the isolated WSL Python environment. The PowerShell runner starts HDFS if it is not already running, uploads the selected CSV, runs Spark against HDFS, and writes the dashboard artifacts under `data/processed/`.

One-time prerequisites in Ubuntu 24.04 WSL:

~~~shell
sudo apt update
sudo apt install -y openjdk-17-jdk python3-venv curl
mkdir -p "$HOME/cybershield-hadoop"
cd "$HOME/cybershield-hadoop"
curl -fLO https://downloads.apache.org/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz
curl -fLO https://downloads.apache.org/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz.sha512
sha512sum -c hadoop-3.5.0.tar.gz.sha512
tar -xzf hadoop-3.5.0.tar.gz
~~~

Run from PowerShell in the project folder:

~~~powershell
.\scripts\run_hadoop_spark.ps1
~~~

To use another labeled CSV:

~~~powershell
.\scripts\run_hadoop_spark.ps1 -CsvPath .\data\raw\your_flows.csv
~~~

The first run formats only the new CyberShield NameNode directory, then creates `/user/cybershield/raw`, uploads the CSV, and writes cleaned Parquet to `/user/cybershield/processed/latest`. MongoDB is skipped unless `MONGODB_URI` is set in WSL; Spark and HDFS work without MongoDB. The NameNode UI is at http://localhost:9870 while HDFS is running. HDFS files persist across runs in `$HOME/cybershield-hdfs-data/`.

The runner creates a WSL venv at `$HOME/cybershield-pyspark-venv` and installs `requirements.txt` there when required. `setuptools` is included because PySpark 3.5.6 uses `distutils.version` for its pandas conversion path on Python 3.12.

Check the HDFS files from an Ubuntu WSL terminal:

~~~shell
source "$HOME/.profile"
hdfs dfs -ls -R /user/cybershield
hdfs dfsadmin -report
~~~

After the run, start or refresh the dashboard with the normal Windows Python command from the no-Docker section above. It reads the same `data/processed/` artifacts.

## Optional Docker Compose route

Requirements for the full-stack path: Docker Desktop with its WSL2 backend on Windows, or Docker Engine plus the Docker Compose plugin on Linux; at least 8 GB RAM is recommended for a comfortable run. Internet is needed once to pull container images and Python packages. Use the WSL/Linux instructions from a WSL/Linux terminal; use PowerShell for the Windows script.

1. Open a terminal in this CyberShield folder. Start the HDFS, MongoDB, and dashboard services:

~~~shell
docker compose up -d --build
~~~

2. Wait until the NameNode page opens at http://localhost:9870. Upload the included sample.

PowerShell:

~~~powershell
powershell -ExecutionPolicy Bypass -File scripts/upload_to_hdfs.ps1 -CsvPath data/sample/sample_traffic.csv
~~~

WSL/Linux:

~~~shell
bash scripts/upload_to_hdfs.sh data/sample/sample_traffic.csv
~~~

3. Run the end-to-end pipeline from the app container:

~~~shell
docker compose exec app python -m cybershield.pipeline
~~~

The command reads the CSV from HDFS, writes cleaned Parquet back to HDFS, trains/evaluates the bounded scikit-learn sample, stores held-out prediction records in MongoDB, and writes local dashboard artifacts. Open http://localhost:8501 and refresh if it was already open.

4. Check HDFS and MongoDB:

~~~shell
docker compose exec namenode hdfs dfs -ls -R /user/cybershield
docker compose exec namenode hdfs dfs -du -h /user/cybershield
docker compose exec namenode hdfs dfsadmin -report
docker compose exec mongodb mongosh CyberShield
~~~

The first pipeline run creates the CyberShield database and collections. Leave the compose volumes in place to keep HDFS and MongoDB data between restarts.

### Hadoop/HDFS file-management examples

All commands below run the actual HDFS CLI inside the NameNode container:

~~~shell
docker compose exec namenode hdfs dfs -mkdir -p /user/cybershield/raw
docker compose exec namenode hdfs dfs -put -f /tmp/a.csv /user/cybershield/raw/a.csv
docker compose exec namenode hdfs dfs -ls /user/cybershield/raw
docker compose exec namenode hdfs dfs -du -h /user/cybershield
docker compose exec namenode hdfs dfs -cat /user/cybershield/raw/sample_traffic.csv
docker compose exec namenode hdfs dfs -get /user/cybershield/processed/latest ./latest-parquet
~~~

HDFS divides a file into blocks, records their locations in the NameNode metadata, and stores block contents on DataNodes. In this one-DataNode teaching setup, the replication factor is one; larger clusters normally use multiple DataNodes and replicas. The NameNode web UI is available at http://localhost:9870.

## Public dataset: UNSW-NB15

UNSW-NB15 is the default benchmark because its labeled CSV training and testing partitions are a manageable starting point. The official UNSW page provides the CSVs and grants use for academic research with required citations. Download the two labeled files named UNSW_NB15_training-set.csv and UNSW_NB15_testing-set.csv from the page's SharePoint download link. Put them in data/raw/.

Official dataset page: https://research.unsw.edu.au/projects/unsw-nb15-dataset

Upload both CSVs, one at a time:

~~~powershell
powershell -ExecutionPolicy Bypass -File scripts/upload_to_hdfs.ps1 -CsvPath data/raw/UNSW_NB15_training-set.csv
powershell -ExecutionPolicy Bypass -File scripts/upload_to_hdfs.ps1 -CsvPath data/raw/UNSW_NB15_testing-set.csv
~~~

The same upload command works for WSL/Linux by using bash scripts/upload_to_hdfs.sh. The default HDFS wildcard reads every CSV in the raw directory. The pipeline combines the selected files, then makes its own reproducible 75/25 train/test split. It does not preserve the benchmark's official partition boundary; mention this in a report and use an untouched external test set for a stricter experiment.

The UNSW schema includes source/destination IP and port, protocol, service, state, duration, bytes, packets, and attack_cat. The attack_cat labels are Normal plus Analysis, Backdoor, DoS, Exploits, Fuzzers, Generic, Reconnaissance, Shellcode, and Worms. Its separate label column is a binary target (0 normal, 1 attack); CyberShield uses attack_cat for multiclass training and translates blank normal categories with the binary label.

The official description reports 175,341 rows in its training partition and 82,332 in its testing partition; it also describes the full source set as 2,540,044 records across four files. Per-class counts depend on which files were downloaded. Do not copy a class distribution or score from a paper: read the counts from data/processed/analytics.json after your run. The supplied sample is synthetic and has 26 rows: Normal 12, DoS 4, Generic 4, Reconnaissance 3, Exploits 3.

### Optional dataset: CIC-IDS2017

The Canadian Institute for Cybersecurity publishes labeled CIC-IDS2017 flow CSVs generated with CICFlowMeter. The files contain fields such as Source IP, Destination IP, Source Port, Destination Port, Protocol, Flow Duration, packet/byte lengths, Timestamp, and Label. Select one or more labeled CSV files from the official page at https://www.unb.ca/cic/datasets/ids-2017.html, download them to data/raw/, and upload each using the same script. The pipeline recognizes common CIC flow names and normalizes BENIGN to Normal. Class distribution depends on the selected day/file; inspect the generated analytics artifact.

Both benchmarks are offline evaluation data, not live threat intelligence. Their IP addresses and labels describe dataset scenarios and should not be treated as current indicators of compromise.

## What the pipeline does

1. Spark reads headered CSV files from the HDFS raw directory.
2. It maps common UNSW-NB15 and CIC-IDS2017 headers to shared fields, removes exact duplicate standardized rows, converts feature columns to numbers, records pre-imputation missing-value counts, imputes numeric missing values with the Spark median (zero where a feature is absent), and fills missing categories/IPs with an unknown marker.
3. It writes standardized cleaned rows as Parquet at HDFS /user/cybershield/processed/latest.
4. Spark computes total, normal, and labeled-attack flow counts; category/protocol distributions; source IP counts among labeled attacks; hourly trends when timestamps parse; and p95/p99 volume thresholds. “Unusual volume” means byte or packet totals at/above the observed p99; it is a simple dataset-relative rule.
5. Spark takes an approximately class-balanced sample capped at 100,000 rows. The pandas/scikit-learn driver step one-hot encodes protocol/service/state, imputes features, splits train/test (stratified when sample sizes allow), and trains a 200-tree Random Forest. Source/destination IPs are excluded from features.
6. It calculates accuracy, macro precision, macro recall, macro F1, a per-class report, and a confusion matrix from the held-out split. Results depend on input quality, labeling, selected files, class imbalance, and the split.
7. It predicts the held-out sample and assigns transparent triage severity: critical for selected high-impact category names; high for other predicted attacks with bytes or packets at/above p95; medium for other predicted attacks; low for Normal. This is a project rule, not a calibrated risk score or an automatic response.
8. It upserts up to 20,000 held-out prediction records to MongoDB. Records predicted as attacks also enter threat_alerts. Run artifacts are written to data/processed/.

Change limits/seed when needed:

~~~shell
docker compose exec app python -m cybershield.pipeline --max-model-sample 50000 --max-mongo-docs 5000 --random-state 7
~~~

Skip MongoDB writes for an offline diagnosis with --skip-mongo. A full-dataset Spark pass still reads and processes every input row; the cap limits only the sklearn driver sample.

## MongoDB

Database: CyberShield

Collections:

| Collection | Contents |
| --- | --- |
| traffic_processed | Held-out rows with actual/predicted category, severity, flow fields, and prediction correctness. |
| threat_alerts | The stored rows predicted as attacks; includes severity and source IP context. |
| analytics_runs | Whole-dataset summary and measured model metrics for each pipeline run. |

Example mongosh queries:

~~~javascript
use CyberShield
db.traffic_processed.find({}, {src_ip: 1, actual_category: 1, predicted_category: 1, severity: 1}).limit(10)
db.threat_alerts.aggregate([
  {$group: {_id: {severity: "$severity", category: "$predicted_category"}, count: {$sum: 1}}},
  {$sort: {count: -1}}
])
db.threat_alerts.aggregate([
  {$group: {_id: "$src_ip", alerts: {$sum: 1}}},
  {$sort: {alerts: -1}}, {$limit: 10}
])
db.analytics_runs.find().sort({generated_at_utc: -1}).limit(1)
~~~

MongoDB can be inspected with mongosh inside its container or MongoDB Compass at mongodb://localhost:27017. This local demonstration has no authentication configured; do not expose it to an untrusted network.

## Dashboard and actual outputs

Open http://localhost:8501. The dashboard shows flow counts, labeled attack and protocol distributions, normal/malicious ratio, hourly trend if a usable timestamp exists, top labeled suspicious source IPs, measured Random Forest metrics, confusion matrix, and held-out predictions with severity.

After a successful pipeline run, these files are generated:

| Artifact | Meaning |
| --- | --- |
| data/processed/analytics.json | Full-dataset counts, distributions, volume thresholds, timestamp availability, detected input columns, and missing-value counts. |
| data/processed/model_metrics.json | Measured held-out model scores, sample/class counts, confusion matrix, classification report, and severity rules. |
| data/processed/predictions.csv | Per-row actual and predicted labels and severity for the held-out model sample. |
| HDFS processed/latest | Cleaned Parquet output from Spark. |
| MongoDB CyberShield | Stored held-out records, alerts, and run summaries. |

The checked-in repository excludes generated run artifacts. A local native WSL run on the included 26-row synthetic sample completed with 26 cleaned flows, 12 normal and 14 labeled attacks, and 7 held-out predictions. The sample split scored 1.0 on accuracy and macro F1; that result is not a meaningful benchmark because the sample is tiny and synthetic. For a project evaluation, use a full labeled benchmark and report its measured metrics. Generate evidence screenshots with Snipping Tool on Windows or the screenshot utility on Linux: HDFS NameNode overview, HDFS raw/processed listing, terminal pipeline completion, and dashboard sections for analytics and model evaluation. Save screenshots in a project evidence folder and record dataset filenames, run date, row counts, and actual output metrics in the report.

## Academic materials

See docs/academic_report.md for the detailed report draft and docs/presentation_and_viva.md for a 12-slide outline, demo sequence, and 20 viva questions with answers. Replace the result placeholders with values from your own run and cite the selected dataset.

## References

- UNSW-NB15 official dataset page and academic-use/citation details: https://research.unsw.edu.au/projects/unsw-nb15-dataset
- CIC-IDS2017 official dataset page: https://www.unb.ca/cic/datasets/ids-2017.html
- Apache Hadoop single-node cluster and HDFS documentation: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html
- Apache Spark documentation: https://spark.apache.org/docs/latest/
- Scikit-learn documentation: https://scikit-learn.org/stable/
- MongoDB manual: https://www.mongodb.com/docs/manual/
- Streamlit documentation: https://docs.streamlit.io/
