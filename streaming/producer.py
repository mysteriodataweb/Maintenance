"""Producteur Kafka : diffuse les mesures capteurs C-MAPSS en temps reel.

Le script lit le fichier de test C-MAPSS du sous-ensemble configure, puis
publie chaque ligne comme un message JSON sur le topic d'entree. Le consumer
(``streaming/consumer.py``) consomme ce topic, calcule les features temporelles
et publie la RUL predite sur le topic de sortie.

Execution
---------
    python -m streaming.producer
"""

from __future__ import annotations

import json
import os
import sys
import time

import pandas as pd
from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import config  # noqa: E402

#: Fichier de test diffuse. Le jeu de test ne contient que l'etat final de
#: chaque moteur, ce qui permet de reproduire les conditions du test officiel.
DATASET_PATH = os.path.join(
    config.RAW_DATA_DIR,
    f"test_{config.DATASET_ID}.txt",
)

#: Delai entre deux publications, en secondes. Simule la cadence d'un moteur
#: reellement instrumente. Mettre à 0 pour saturer le topic le plus vite possible.
PRODUCER_INTERVAL_SECONDS = float(
    os.environ.get("MAINTENANCE_PRODUCER_INTERVAL", "0.1")
)


def build_producer() -> KafkaProducer:
    """Cree un producteur Kafka serialisant les messages en JSON UTF-8."""
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def load_sensor_data(dataset_path: str = DATASET_PATH) -> pd.DataFrame:
    """Charge les mesures brutes C-MAPSS depuis un fichier texte espace-separated."""
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(
            f"Fichier introuvable : {dataset_path}\n"
            "Telechargez le jeu de donnees avec : python -m src.download"
        )

    df = pd.read_csv(
        dataset_path,
        sep=r"\s+",
        header=None,
        names=config.COLUMN_NAMES,
    )

    print(f"{len(df)} observations chargees depuis {dataset_path}.")
    return df


def main() -> None:
    """Publie chaque ligne du jeu de test sur le topic des mesures."""
    df = load_sensor_data()

    producer = build_producer()

    print(f"Topic source   : {config.SENSOR_TOPIC}")
    print(f"Broker Kafka   : {config.KAFKA_BOOTSTRAP_SERVERS}")
    print("Debut du streaming...\n")

    for _, row in df.iterrows():
        message = row.to_dict()

        producer.send(config.SENSOR_TOPIC, value=message)

        print(
            f"Envoye -> moteur {message['unit_number']} | "
            f"cycle {message['time_in_cycles']}"
        )

        if PRODUCER_INTERVAL_SECONDS > 0:
            time.sleep(PRODUCER_INTERVAL_SECONDS)

    producer.flush()
    producer.close()

    print("\nStreaming termine.")


if __name__ == "__main__":
    main()
