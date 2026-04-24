import os
import time
import json
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC")
API_URL                 = os.getenv("API_URL")
POLL_INTERVAL_SECONDS   = int(os.getenv("POLL_INTERVAL_SECONDS"))

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err:
        print(f"[PRODUCER] Echec livraison : {err}")
    else:
        print(f"[PRODUCER] Message publié -> topic={msg.topic()} offset={msg.offset()}")


def fetch_transaction() -> dict | None:
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        
        payload = response.json()
        
        # L'API retourne parfois une string JSON encodée deux fois
        if isinstance(payload, str):
            import json
            payload = json.loads(payload)
            
        columns = payload["columns"]
        data    = payload["data"][0]
        return dict(zip(columns, data))
    except requests.exceptions.RequestException as e:
        print(f"[PRODUCER] Erreur API : {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"[PRODUCER] Format de réponse inattendu : {e}")
        return None


def main():
    print(f"[PRODUCER] Démarré | topic={KAFKA_TOPIC} | poll toutes les {POLL_INTERVAL_SECONDS}s")

    while True:
        transaction = fetch_transaction()

        if transaction:
            trans_num = transaction.get("trans_num", "unknown")
            print(f"[PRODUCER] Transaction récupérée : {trans_num} | montant={transaction.get('amt')} | marchand={transaction.get('merchant')}")

            producer.produce(
                topic=KAFKA_TOPIC,
                key=trans_num,
                value=json.dumps(transaction),
                callback=delivery_report
            )
            producer.poll(0)

        else:
            print("[PRODUCER] Aucune transaction récupérée, nouvelle tentative dans 5s")
            time.sleep(5)
            continue

        time.sleep(POLL_INTERVAL_SECONDS)


main()