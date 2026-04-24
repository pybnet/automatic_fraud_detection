import os
import json
import threading
import time
from datetime import datetime
from collections import deque

import pandas as pd
import plotly.express as px
import streamlit as st
from confluent_kafka import Consumer
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "fraud-predictions")
DB_URI                  = os.getenv("DB_URI", "postgresql://fraud_user:fraud_pass@localhost:5432/fraud_db")

# Buffer des derniers messages Kafka reçus (thread-safe via deque)
LIVE_BUFFER = deque(maxlen=50)

# Thread Kafka pour écouter les nouvelles prédictions en continu
def kafka_listener():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id":          "streamlit-dashboard",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([KAFKA_TOPIC])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg and not msg.error():
            try:
                data = json.loads(msg.value().decode("utf-8"))
                data["received_at"] = datetime.now().strftime("%H:%M:%S")
                LIVE_BUFFER.appendleft(data)
            except Exception:
                pass


# Démarrer le thread Kafka une seule fois
if "kafka_thread_started" not in st.session_state:
    t = threading.Thread(target=kafka_listener, daemon=True)
    t.start()
    st.session_state["kafka_thread_started"] = True



# Chargement historiques
@st.cache_resource
def get_engine():
    return create_engine(DB_URI)

def load_count(engine) -> int:
    query = text("SELECT COUNT(*) FROM predictions")
    with engine.connect() as conn:
        return conn.execute(query).scalar()

def load_history(engine, limit=500) -> pd.DataFrame:
    query = text("""
        SELECT trans_num, merchant, category, amt, city, state,
               is_fraud_true, is_fraud_pred, fraud_score, predicted_at
        FROM predictions
        ORDER BY predicted_at DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"limit": limit})


# Interface Streamlit
st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon=":shield:",
    layout="wide"
)

st.title("Fraud Detection — Dashboard temps réel")

engine = get_engine()

try:
    df_hist = load_history(engine)
    has_data = len(df_hist) > 0
except Exception as e:
    st.warning(f"Impossible de charger l'historique Postgres : {e}")
    df_hist = pd.DataFrame()
    has_data = False

# KPIS
col1, col2, col3, col4 = st.columns(4)

if has_data:
    total = load_count(engine)
    n_fraud = int(df_hist["is_fraud_pred"].sum())
    taux_fraude = n_fraud / total * 100
    amt_moyen= df_hist["amt"].mean()

    col1.metric("Transactions analysées", total)
    col2.metric("Fraudes détectées", n_fraud)
    col3.metric("Taux de fraude", f"{taux_fraude:.1f}%")
    col4.metric("Montant moyen", f"${amt_moyen:.2f}")
else:
    col1.metric("Transactions analysées", "—")
    col2.metric("Fraudes détectées", "—")
    col3.metric("Taux de fraude", "—")
    col4.metric("Montant moyen", "—")

st.divider()

# Flux en direct de kafka
st.subheader("Flux en direct")

live_data = list(LIVE_BUFFER)

if live_data:
    df_live = pd.DataFrame(live_data)

    def highlight_fraud(row):
        color = "background-color: #ffd6d6" if row.get("is_fraud_pred") == 1 else ""
        return [color] * len(row)

    st.dataframe(
        df_live[["received_at", "trans_num", "merchant", "category", "amt", "is_fraud_pred", "fraud_score"]],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("En attente de transactions... (le consumer doit être actif)")

st.divider()

#Graph
if has_data:
    st.subheader("Historique des prédictions")

    col_a, col_b = st.columns(2)

    with col_a:
        fig_cat = px.bar(
            df_hist.groupby("category")["is_fraud_pred"].sum().reset_index(),
            x="category", y="is_fraud_pred",
            labels={"is_fraud_pred": "Fraudes détectées", "category": "Catégorie"},
            title="Fraudes par catégorie",
            color_discrete_sequence=["#E24B4A"]
        )
        fig_cat.update_layout(xaxis_tickangle=-45, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig_cat, use_container_width=True)

    with col_b:
        df_score = df_hist.copy()
        df_score["fraud_score"] = df_score["fraud_score"].clip(0, 1)
        df_score["Statut"] = df_score["is_fraud_pred"].map({0: "Légitime", 1: "Fraude"})
        fig_score = px.histogram(
            df_score,
            x="fraud_score",
            color="Statut",
            nbins=20,
            range_x=[0, 1],
            labels={"fraud_score": "Score de fraude (0 = légitime, 1 = fraude)"},
            title="Distribution des scores de fraude",
            color_discrete_map={"Légitime": "#378ADD", "Fraude": "#E24B4A"}
        )
        fig_score.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            xaxis=dict(range=[0, 1], tickformat=".1f"),
            bargap=0.05
        )
        st.plotly_chart(fig_score, use_container_width=True)

    st.subheader("Dernières transactions")
    st.dataframe(df_hist, use_container_width=True, hide_index=True)

#Autorefresh
time.sleep(5)
st.rerun()
