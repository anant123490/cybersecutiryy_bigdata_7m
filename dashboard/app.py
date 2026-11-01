"""Streamlit dashboard for CyberShield pipeline artifacts."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

OUTPUT_DIR = Path(os.getenv("LOCAL_OUTPUT_DIR", "data/processed"))
ANALYTICS_FILE = OUTPUT_DIR / "analytics.json"
METRICS_FILE = OUTPUT_DIR / "model_metrics.json"
PREDICTIONS_FILE = OUTPUT_DIR / "predictions.csv"

st.set_page_config(
    page_title="CyberShield | Threat Analytics",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(
    """
    <style>
      .stApp { background: #08111f; color: #e6edf7; }
      [data-testid="stSidebar"] { background: #0d1929; }
      [data-testid="stMetric"] { background: #101e30; border: 1px solid #203148; padding: 16px; border-radius: 12px; }
      h1, h2, h3 { color: #e6edf7; }
      div[data-testid="stMetricLabel"] p { color: #9fb1c8; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=15)
def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


@st.cache_data(ttl=15)
def load_predictions(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


st.title("🛡️ CyberShield")
st.caption("Offline network intrusion detection and cyber threat analytics")

with st.sidebar:
    st.subheader("Data source")
    st.write("Offline labeled flow CSV input; the latest run mode is shown below.")
    if st.button("Refresh dashboard"):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("Severity is a rule-based triage aid. It is not an automated response.")

if not ANALYTICS_FILE.exists() or not METRICS_FILE.exists():
    st.info(
        "No pipeline results are available yet. Follow the Quick start in the project README "
        "to put the included synthetic sample in HDFS and run the analytics pipeline."
    )
    st.code(
        "docker compose up -d --build\n"
        "powershell -ExecutionPolicy Bypass -File scripts/upload_to_hdfs.ps1 "
        "-CsvPath data/sample/sample_traffic.csv\n"
        "docker compose exec app python -m cybershield.pipeline",
        language="powershell",
    )
    st.stop()

analytics = load_json(str(ANALYTICS_FILE))
metrics = load_json(str(METRICS_FILE))
predictions = load_predictions(str(PREDICTIONS_FILE)) if PREDICTIONS_FILE.exists() else pd.DataFrame()
with st.sidebar:
    st.caption(f"Run mode: {analytics.get('execution_mode', 'not recorded')}")
    if analytics.get("mongodb_write", {}).get("status") == "skipped":
        st.caption("MongoDB persistence skipped for this local run.")
total = int(analytics.get("flows_after_cleaning", 0))
normal = int(analytics.get("normal_flows", 0))
attacks = int(analytics.get("labeled_attack_flows", 0))

st.caption(
    f"Latest run: {analytics.get('generated_at_utc', 'unknown time')} UTC · "
    f"{analytics.get('execution_mode', 'run mode not recorded')} · "
    f"{analytics.get('raw_rows_read', 0):,} input rows · "
    f"{analytics.get('duplicate_raw_rows_removed', 0):,} exact raw duplicates removed"
)

cards = st.columns(4)
cards[0].metric("Flows analyzed", f"{total:,}")
cards[1].metric("Normal flows", f"{normal:,}")
cards[2].metric("Labeled attacks", f"{attacks:,}")
cards[3].metric("Unusual volume", f"{analytics.get('unusual_volume_flows', 0):,}")

st.subheader("Traffic overview")
left, right = st.columns(2)
distribution = analytics.get("attack_distribution", [])
protocols = analytics.get("protocol_distribution", [])
normal_attack = pd.DataFrame([
    {"class": "Normal", "flows": normal},
    {"class": "Labeled attack", "flows": attacks},
])
with left:
    st.plotly_chart(
        px.bar(
            pd.DataFrame(distribution), x="attack_category", y="count",
            color="attack_category", title="Attack category distribution",
            template="plotly_dark",
        ).update_layout(showlegend=False, xaxis_title="", yaxis_title="Flows"),
        use_container_width=True,
    )
with right:
    st.plotly_chart(
        px.pie(
            normal_attack, names="class", values="flows",
            title="Normal versus malicious traffic", hole=0.48,
            color="class", color_discrete_map={"Normal": "#25c685", "Labeled attack": "#ff5d6c"},
            template="plotly_dark",
        ),
        use_container_width=True,
    )

protocol_col, trend_col = st.columns(2)
with protocol_col:
    st.plotly_chart(
        px.bar(
            pd.DataFrame(protocols), x="protocol", y="count", color="protocol",
            title="Protocol distribution", template="plotly_dark",
        ).update_layout(showlegend=False, xaxis_title="", yaxis_title="Flows"),
        use_container_width=True,
    )
with trend_col:
    trend = analytics.get("traffic_trend", [])
    if trend:
        trend_frame = pd.DataFrame(trend)
        trend_frame["hour"] = pd.to_datetime(trend_frame["hour"], errors="coerce")
        trend_long = trend_frame.melt(
            id_vars=["hour"], value_vars=["count", "attack_count"],
            var_name="series", value_name="flows",
        )
        st.plotly_chart(
            px.line(
                trend_long, x="hour", y="flows", color="series", markers=True,
                title="Flow trends by hour", template="plotly_dark",
                color_discrete_map={"count": "#5aa9ff", "attack_count": "#ff5d6c"},
            ).update_layout(xaxis_title="", yaxis_title="Flows"),
            use_container_width=True,
        )
    else:
        st.info("No parseable event timestamps were present for a time trend.")

st.subheader("Model evaluation")
st.caption(
    f"{metrics.get('model', 'Model')} · {metrics.get('task', '')} · "
    f"{metrics.get('training_rows', 0):,} train / {metrics.get('test_rows', 0):,} held-out rows. "
    "These are measured results from the last run, not expected or prefilled scores."
)
metric_cols = st.columns(4)
for col, key, label in zip(
    metric_cols,
    ["accuracy", "precision_macro", "recall_macro", "f1_macro"],
    ["Accuracy", "Macro precision", "Macro recall", "Macro F1"],
):
    col.metric(label, f"{float(metrics.get(key, 0.0)):.3f}")

st.markdown("#### Severity indicators — held-out prediction sample")
severity_values = (
    predictions["severity"].astype(str).str.lower().value_counts()
    if not predictions.empty and "severity" in predictions
    else pd.Series(dtype="int64")
)
severity_cols = st.columns(4)
for col, severity in zip(severity_cols, ["critical", "high", "medium", "low"]):
    col.metric(severity.title(), f"{int(severity_values.get(severity, 0)):,}")

labels = metrics.get("labels", [])
matrix = metrics.get("confusion_matrix", [])
if labels and matrix:
    heat = go.Figure(data=go.Heatmap(
        z=matrix, x=labels, y=labels, colorscale="Blues",
        text=matrix, texttemplate="%{text}", hovertemplate="Actual %{y}<br>Predicted %{x}<br>Count %{z}<extra></extra>",
    ))
    heat.update_layout(
        title="Confusion matrix (rows: actual, columns: predicted)",
        template="plotly_dark", xaxis_title="Predicted category",
        yaxis_title="Actual category", height=480,
    )
    st.plotly_chart(heat, use_container_width=True)

ip_col, alert_col = st.columns([1, 1.5])
with ip_col:
    st.subheader("Top suspicious source IPs")
    ips = analytics.get("top_suspicious_source_ips", [])
    if ips:
        st.plotly_chart(
            px.bar(
                pd.DataFrame(ips), x="count", y="src_ip", orientation="h",
                title="Flows labeled as attacks", template="plotly_dark",
            ).update_layout(yaxis={"categoryorder": "total ascending"}, xaxis_title="Labeled attack flows", yaxis_title=""),
            use_container_width=True,
        )
    else:
        st.info("No source IP values were available for ranking.")

with alert_col:
    st.subheader("Held-out suspicious predictions")
    if not predictions.empty and "predicted_is_attack" in predictions:
        suspicious = predictions[predictions["predicted_is_attack"].astype(str).str.lower().isin(["true", "1"])]
        show_cols = [
            col for col in [
                "src_ip", "dst_ip", "protocol", "src_port", "dst_port",
                "actual_category", "predicted_category", "severity", "prediction_correct",
            ] if col in suspicious.columns
        ]
        st.dataframe(
            suspicious[show_cols].head(100),
            use_container_width=True,
            hide_index=True,
        )
        st.caption("Rows are model predictions on the held-out portion of the bounded sample.")
    else:
        st.info("No prediction rows are available.")

with st.expander("Severity rules and model details"):
    st.json(metrics.get("severity_rules", {}))
    st.write(metrics.get("threshold_note", ""))
    st.write(
        "The model uses flow-level ports, duration, byte/packet counts, protocol, service, and "
        "state. IP addresses are retained for aggregation and alert context but excluded from "
        "model features to reduce identity memorization."
    )
    st.write(
        "Class imbalance and dataset artifacts can materially change the scores. Review per-class "
        "precision/recall in the saved classification report before interpreting the aggregate metrics."
    )
