# Presentation outline, demonstration, and viva

## 12-slide presentation content

### Slide 1 — CyberShield

- Network Intrusion Detection and Cyber Threat Analytics Using Hadoop and Apache Spark
- Student, roll number, department, guide, academic year
- Defensive, offline benchmark analysis

### Slide 2 — Problem and motivation

- Labeled flow files contain many connections and multiple traffic features.
- Manual review becomes difficult as row counts grow.
- Need a repeatable process for storage, cleaning, analytics, classification, and review.

### Slide 3 — Project objectives

- Use HDFS for raw and processed dataset storage.
- Use Spark for data cleaning and traffic analytics.
- Train and evaluate a multiclass model.
- Store selected records and show results in a dashboard.

### Slide 4 — Dataset

- State the exact public dataset and files used.
- UNSW-NB15 labels: Normal plus nine attack categories.
- Fields: endpoint IPs/ports, protocol, state/service, duration, bytes, packets, category.
- Show actual class counts from analytics.json; avoid estimated or paper-reported performance.

### Slide 5 — System architecture

- Local CSV → HDFS raw → PySpark → cleaned Parquet in HDFS.
- Spark summary + bounded sample → Random Forest.
- Predictions → MongoDB and JSON/CSV → Streamlit + Plotly.
- Note that Spark is local-mode and HDFS has one DataNode in this demo.

### Slide 6 — Hadoop and Spark roles

- NameNode tracks HDFS metadata and block locations.
- DataNode stores file blocks.
- HDFS CLI verifies upload and output.
- Spark DataFrames process the HDFS data; Spark does not replace HDFS storage.

### Slide 7 — Preprocessing and analytics

- Normalize UNSW/CIC headers and labels.
- Remove duplicates; median-impute numeric missing values.
- Count attacks and protocols; rank source IPs associated with labeled attacks.
- Mark volume outliers at dataset p99 and plot hourly trends when timestamp exists.

### Slide 8 — Machine learning

- Random Forest multiclass classifier with categorical one-hot encoding.
- Numeric fields: ports, duration, bytes, packets.
- Exclude IP addresses as model inputs.
- Capped approximately class-balanced sample; reproducible held-out split.

### Slide 9 — Evaluation

- Show actual accuracy, macro precision, macro recall, and macro F1.
- Show confusion matrix and per-class report.
- Discuss class imbalance and the difference between aggregate and minority-class performance.
- Insert values only after executing the pipeline.

### Slide 10 — MongoDB and dashboard

- traffic_processed, threat_alerts, analytics_runs.
- Dashboard charts for attack category, protocol, traffic trend, source IPs, metrics, and severity.
- Severity is a rule-based review aid; it does not trigger a response.

### Slide 11 — Results, limits, and future work

- Present actual row counts, class distribution, metrics, and stored-output counts.
- Limits: offline benchmark, single-machine Spark, one DataNode, possible label artifacts.
- Future: multi-node cluster, official-split/time-based evaluation, drift and analyst review.

### Slide 12 — Conclusion and references

- Summarize the HDFS → Spark → ML → MongoDB → dashboard flow.
- Cite the selected dataset and Apache/scikit-learn documentation.
- Invite questions.

## Demonstration procedure

Run this before presenting so Docker images and the dataset are already available:

1. Show the project folder and the small synthetic input or identify the downloaded benchmark filenames.
2. Open the NameNode UI at http://localhost:9870 and show HDFS health.
3. In a terminal, list /user/cybershield/raw and show that a CSV was stored in HDFS.
4. Run docker compose exec app python -m cybershield.pipeline. Point out Spark's HDFS input/output and the generated metric artifact.
5. Show the cleaned Parquet directory with hdfs dfs -ls -R /user/cybershield/processed.
6. Open the dashboard at http://localhost:8501. Explain the class distribution, model scores, confusion matrix, top dataset source IPs, and rule-based severity.
7. Show MongoDB collections using mongosh CyberShield, and run one alert aggregation query from README.
8. State whether the data was synthetic or benchmark data, the exact row counts, the sample cap, and the class distribution. Explain the single-machine compute limitation.

If using only the sample, explicitly call it synthetic workflow data and avoid describing its tiny scores as evidence of a detector's real-world quality.

## 20 viva questions with answers

1. **What problem does CyberShield address?**  
   It demonstrates an offline big-data workflow that summarizes labeled network flows, classifies flow categories, stores selected predictions, and visualizes results.

2. **Why use HDFS?**  
   HDFS is a distributed filesystem designed to store large files as blocks across DataNodes. This project demonstrates HDFS upload, listing, reading, and output.

3. **What does the NameNode do?**  
   It manages filesystem metadata and tracks where file blocks are stored; it does not normally store the user file block contents.

4. **What does a DataNode do?**  
   It stores HDFS blocks and serves read/write operations for those blocks.

5. **Is this a multi-node project?**  
   No. It uses a real HDFS NameNode and one DataNode, while Spark runs in local mode on one machine. It demonstrates the data path, not a fault-tolerant multi-worker cluster.

6. **Why use Spark DataFrames?**  
   DataFrames provide distributed, optimized transformations and SQL-like grouping for larger datasets without manually looping through every row in Python.

7. **Why store cleaned data as Parquet?**  
   Parquet is a columnar format useful for analytics because readers can access selected columns and retain a typed schema more efficiently than repeatedly parsing CSV.

8. **Which dataset is used?**  
   The configured default is UNSW-NB15; CIC-IDS2017 is also supported for common flow fields. The final report must state the exact files actually used.

9. **What is the classification target?**  
   The attack category, with Normal as the benign class. In UNSW-NB15, attack_cat is used instead of the separate binary label.

10. **Why are source and destination IPs not model features?**  
    They are useful for aggregation and alert context, but can let a model memorize dataset-specific hosts. Excluding them reduces that risk.

11. **How are missing numeric values handled?**  
    Spark computes a median for each available numeric feature; if the source dataset omits the feature, its value defaults to zero. Missing categorical fields use unknown.

12. **How are duplicates handled?**  
    The standardized flow rows are deduplicated before downstream statistics and Parquet output. The run summary also reports exact duplicate raw rows.

13. **How is the training sample bounded?**  
    Spark samples approximately evenly across labels and caps the sample at 100,000 rows by default before collecting for scikit-learn.

14. **Why use a train-test split?**  
    To measure predictions on rows not used to fit the model. The split is reproducible and stratified when class support allows.

15. **What does precision measure?**  
    Of the rows predicted as a class, the fraction that actually belong to that class.

16. **What does recall measure?**  
    Of the actual rows in a class, the fraction the model found.

17. **Why report macro F1?**  
    It averages F1 across classes equally, making minority-class performance visible instead of letting the majority class dominate.

18. **What is a confusion matrix?**  
    A table of actual versus predicted classes. The diagonal contains correct predictions; off-diagonal cells reveal class confusions.

19. **How is severity assigned?**  
    It is a documented heuristic based on predicted category and dataset p95 volume thresholds. It is for triage demonstration and is not a calibrated risk score.

20. **What are the main limitations and next steps?**  
    The benchmark may be imbalanced or dated, and this run is offline with single-machine compute. Better future evaluation includes the official split, time-separated tests, multiple datasets, and a secure multi-node deployment.
