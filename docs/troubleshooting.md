# Troubleshooting

## Docker is not running

Start Docker Desktop (Windows/macOS) or Docker Engine (Linux), then run docker compose ps. The Docker Compose plugin is required.

## Upload says NameNode is not ready

Run docker compose ps and docker compose logs namenode datanode. Wait for the NameNode web page at http://localhost:9870 to load, then rerun the upload command. The NameNode and DataNode keep state in named Docker volumes.

## HDFS reports safe mode

Wait briefly for the DataNode to register. Inspect docker compose exec namenode hdfs dfsadmin -report. For the single-node demo, once the DataNode is live, leave safe mode with:

~~~shell
docker compose exec namenode hdfs dfsadmin -safemode leave
~~~

Do not delete HDFS volumes to fix a transient startup issue; first inspect container logs and status.

## Spark cannot resolve namenode or datanode

Run the pipeline inside the app container using docker compose exec app ... . Docker service names resolve inside the compose network. A host-installed PySpark session may not resolve DataNode block locations correctly.

## No input path matches

Check exact HDFS directory and file names:

~~~shell
docker compose exec namenode hdfs dfs -ls -R /user/cybershield
~~~

The default Spark input is /user/cybershield/raw/*.csv. Upload original CSV files, not a ZIP archive. For a custom path, pass --input hdfs://namenode:9000/path/to/*.csv to the pipeline.

## The pipeline says no label column was found

Use a labeled flow CSV with an attack_cat, attack_category, or Label column. The provided UNSW-NB15 and CIC-IDS2017 flow files have labels. Packet capture (.pcap) files are not CSV flow records and need a separate feature extraction step.

## Model reports only one class

At least two distinct labels must remain after cleaning. Check whether the uploaded file contains both benign/normal and attack rows. For a tiny dataset, combine a few labeled CSVs or use the included sample.

## MongoDB connection timeout

Check docker compose ps mongodb and docker compose logs mongodb. In this project the container address is mongodb; localhost is for client software running on the host. To skip persistence for a diagnostic run, pass --skip-mongo.

## Dashboard has no charts

The dashboard reads data/processed/analytics.json, model_metrics.json, and predictions.csv. Run the pipeline after each new HDFS upload, then refresh the dashboard. It does not fetch live results from HDFS.

## Large files or memory pressure

Spark computes full-dataset summaries in HDFS-backed DataFrames. The scikit-learn stage collects at most 100,000 rows by default. Reduce it with --max-model-sample 20000, increase Docker's memory allocation if available, and avoid concurrent model jobs.

## Resetting demo state

docker compose down stops services and keeps named volumes. docker compose down -v removes the local HDFS and MongoDB volumes, including uploaded files and persisted records. Use the latter only when you intentionally want a clean reset.
