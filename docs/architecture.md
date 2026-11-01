# CyberShield architecture

~~~mermaid
flowchart LR
  CSV[Offline labeled CSV<br/>UNSW-NB15 or CIC-IDS2017] -->|HDFS put| HDFS[(Hadoop HDFS<br/>NameNode + DataNode)]
  HDFS -->|hdfs://namenode:9000| SPARK[PySpark DataFrame pipeline]
  SPARK --> CLEAN[Cleaned, standardized Parquet<br/>in HDFS]
  SPARK --> ANALYTICS[Full-dataset analytics<br/>labels, protocols, IPs, trends]
  SPARK --> SAMPLE[Bounded, approximately stratified sample]
  SAMPLE --> SK[scikit-learn Random Forest<br/>train/test + metrics]
  SK --> PRED[Held-out predictions + severity rules]
  PRED --> MONGO[(MongoDB<br/>CyberShield database)]
  ANALYTICS --> FILES[Local JSON/CSV run artifacts]
  SK --> FILES
  MONGO --> DASH[Streamlit + Plotly dashboard]
  FILES --> DASH
~~~

## Component responsibilities

| Component | Responsibility |
| --- | --- |
| Hadoop HDFS | Persists raw CSV input and cleaned Parquet output as HDFS files. This compose setup has one NameNode and one DataNode; replication is set to one for a laptop. |
| PySpark | Reads CSV from HDFS, standardizes common UNSW-NB15/CIC-IDS2017 field names, removes exact duplicate standardized rows, imputes numeric nulls with medians, and computes whole-dataset traffic statistics. |
| scikit-learn | Fits a multiclass Random Forest on a capped, approximately class-balanced sample collected from Spark; tests it on a held-out split and reports measured metrics. IP fields are omitted from model features. |
| MongoDB | Stores held-out prediction rows in the traffic_processed collection, predicted attacks in threat_alerts, and run analytics in analytics_runs. |
| Streamlit and Plotly | Renders JSON and CSV run artifacts as a dashboard. It is a monitoring view for offline analysis, not a live packet sensor. |

## Data flow and limits

The containerized Spark session uses local[*] and accesses a real HDFS NameNode/DataNode over the Docker network. HDFS is genuinely used for input and cleaned output, while Spark compute is local to one machine. This is suitable for a college demonstration; it is not a multi-worker production cluster. The dashboard displays the latest run's local artifacts. MongoDB stores only up to the configured number of held-out predictions, with attack predictions copied to the alert collection.

Source and destination IPs are included for grouping and alert context. They are not classifier inputs, which reduces the chance that the model memorizes dataset-specific hosts. “Suspicious source IP” means an IP associated with labeled attack flows in the supplied benchmark; it is not a live reputation verdict.

## HDFS layout

~~~text
/user/cybershield/
├── raw/                         # input CSV files
└── processed/
    └── latest/                  # cleaned Parquet files from the most recent run
~~~
