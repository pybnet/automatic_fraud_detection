import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

import numpy as np
import pandas as pd
import mlflow.pyfunc
import xgboost as xgb
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer, KafkaException
from sqlalchemy import create_engine, text

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_IN          = os.getenv("KAFKA_TOPIC_IN")
KAFKA_TOPIC_OUT         = os.getenv("KAFKA_TOPIC_OUT")
MLFLOW_TRACKING_URI     = os.getenv("MLFLOW_TRACKING_URI")
MLFLOW_MODEL_URI        = os.getenv("MLFLOW_MODEL_URI")
DB_URI                  = os.getenv("DB_URI")
SMTP_USER               = os.getenv("SMTP_USER")
SMTP_PASSWORD           = os.getenv("SMTP_PASSWORD")
ALERT_TO                = os.getenv("ALERT_TO")

# Features engineering - liste des catégories à encoder
CATEGORY_COLS = [
    "entertainment", "food_dining", "gas_transport", "grocery_net",
    "grocery_pos", "health_fitness", "home", "kids_pets", "misc_net",
    "misc_pos", "personal_care", "shopping_net", "shopping_pos", "travel"
]

def preprocess(transaction: dict) -> pd.DataFrame:
    row = {}

    row["amt"] = float(transaction.get("amt", 0))
    row["city_pop"] = int(transaction.get("city_pop", 0))

    # Distance géographique cardholder <-> merchant
    lat  = float(transaction.get("lat", 0))
    lon  = float(transaction.get("long", 0))
    mlat = float(transaction.get("merch_lat", 0))
    mlon = float(transaction.get("merch_long", 0))
    row["geo_distance"] = np.sqrt((lat - mlat) ** 2 + (lon - mlon) ** 2)

    # Heure de la transaction
    current_time = transaction.get("current_time", 0)
    try:
        dt = datetime.fromtimestamp(int(current_time) / 1000)
        row["hour"] = dt.hour
    except Exception:
        row["hour"] = 0

    # Encodage one-hot de la catégorie
    category = transaction.get("category", "")
    for cat in CATEGORY_COLS:
        row[f"category_{cat}"] = 1 if category == cat else 0

    df = pd.DataFrame([row])
    df["amt"]          = df["amt"].astype("float64")
    df["city_pop"]     = df["city_pop"].astype("int64")
    df["geo_distance"] = df["geo_distance"].astype("float64")
    df["hour"]         = df["hour"].astype("int32")
    for cat in CATEGORY_COLS:
        df[f"category_{cat}"] = df[f"category_{cat}"].astype("bool")
    return df


# Envoie Email d'alerte en cas de fraude détectée
def send_alert_email(transaction: dict, score: float):
    if not SMTP_USER or not ALERT_TO:
        print("[CONSUMER] Email non configuré, alerte ignorée")
        return

    trans_num = transaction.get("trans_num", "N/A")
    amt       = transaction.get("amt", "N/A")
    merchant  = transaction.get("merchant", "N/A")
    city      = transaction.get("city", "N/A")
    state     = transaction.get("state", "N/A")

    subject = f"[ALERTE FRAUDE] Transaction {trans_num}"
    body = f"""
    Une transaction suspecte a été détectée.

    Transaction : {trans_num}
    Montant     : ${amt}
    Marchand    : {merchant}
    Lieu        : {city}, {state}
    Score fraude: {score:.4f}
    Heure       : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    """

    msg = MIMEMultipart()
    msg["From"]    = SMTP_USER
    msg["To"]      = ALERT_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    print("E-mail envoyé avec succès!")
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, ALERT_TO, msg.as_string())
        print(f"[CONSUMER] Email d'alerte envoyé pour {trans_num}")
    except Exception as e:
        print(f"[CONSUMER] Echec envoi email : {e}")


# Stockage des résultats dans PostgreSQL
def save_to_postgres(engine, transaction: dict, is_fraud_pred: int, fraud_score: float):
    query = text("""
        INSERT INTO predictions
            (trans_num, cc_num, merchant, category, amt, city, state,
             is_fraud_true, is_fraud_pred, fraud_score)
        VALUES
            (:trans_num, :cc_num, :merchant, :category, :amt, :city, :state,
             :is_fraud_true, :is_fraud_pred, :fraud_score)
        ON CONFLICT (trans_num) DO NOTHING
    """)
    with engine.connect() as conn:
        conn.execute(query, {
            "trans_num":     transaction.get("trans_num"),
            "cc_num":        transaction.get("cc_num"),
            "merchant":      transaction.get("merchant"),
            "category":      transaction.get("category"),
            "amt":           transaction.get("amt"),
            "city":          transaction.get("city"),
            "state":         transaction.get("state"),
            "is_fraud_true": transaction.get("is_fraud", -1),
            "is_fraud_pred": is_fraud_pred,
            "fraud_score":   fraud_score,
        })
        conn.commit()


# Main
def main():
    print(f"[CONSUMER] Chargement du modèle depuis MLflow : {MLFLOW_MODEL_URI}")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    pyfunc_model = mlflow.pyfunc.load_model(MLFLOW_MODEL_URI)
    booster = pyfunc_model._model_impl.xgb_model
    print("[CONSUMER] Modèle chargé avec succès")

    engine = create_engine(DB_URI)
    print("[CONSUMER] Connexion PostgreSQL établie")
 
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           "fraud-consumer-group",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC_IN])
    print(f"[CONSUMER] Abonné au topic : {KAFKA_TOPIC_IN}")
 
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
 
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
 
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
 
            transaction  = json.loads(msg.value().decode("utf-8"))
            trans_num    = transaction.get("trans_num", "unknown")
            print(f"[CONSUMER] Message reçu : {trans_num}")
 
            features      = preprocess(transaction)
            dmatrix       = xgb.DMatrix(features)
            fraud_score   = float(booster.predict_proba(features)[0][1])
            is_fraud_pred = 1 if fraud_score >= 0.5 else 0
 
            print(f"[CONSUMER] {trans_num} | score={fraud_score:.4f} | is_fraud={is_fraud_pred}")
 
            result = {
                "trans_num":     trans_num,
                "amt":           transaction.get("amt"),
                "merchant":      transaction.get("merchant"),
                "category":      transaction.get("category"),
                "is_fraud_pred": is_fraud_pred,
                "fraud_score":   fraud_score,
                "predicted_at":  datetime.now().isoformat(),
            }
            producer.produce(
                topic=KAFKA_TOPIC_OUT,
                key=trans_num,
                value=json.dumps(result)
            )
            producer.poll(0)
 
            save_to_postgres(engine, transaction, is_fraud_pred, fraud_score)
 
            if is_fraud_pred == 1:
                print(f"[CONSUMER] FRAUDE DETECTEE : {trans_num} (score={fraud_score:.4f})")
                send_alert_email(transaction, fraud_score)
 
    except KeyboardInterrupt:
        print("[CONSUMER] Arrêt")
    finally:
        consumer.close()
        producer.flush()
        engine.dispose()


main()
