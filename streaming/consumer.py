"""Consumer Kafka : calcule la RUL predite en temps reel et la republie.

Fonctionnement
-------------
1. Consomme le topic des mesures capteurs (``sensor-data``).
2. Accumule l'historique de chaque moteur en memoire.
3. Des qu'un moteur dispose de suffisamment de cycles, recalcule les
   features temporelles (meme code que le notebook d'exploration).
4. Applique le modele Random Forest de regression pour predire la RUL.
5. Republie la prediction sur le topic ``rul-predictions``.

Le modele et la liste ordonnee des features sont charges une seule fois au
demarrage depuis ``models/random_forest_<DATASET_ID>.joblib``.

Execution
---------
    python -m streaming.consumer

Note de production
------------------
L'historique est conserve en memoire : au redemarrage, chaque moteur doit
recommencer a accumuler ``MIN_HISTORY_CYCLES`` observations. Pour un service
durable, brancher un store de series temporelles (Redis, InfluxDB, TimescaleDB)
ou un topic de rejeu.
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict

import joblib
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import config  # noqa: E402

#: Capteurs retenus pour l'ingenierie des caracteristiques. Les capteurs
#: constants de FD001 (1, 10, 18, 19) sont exclus : ils n'apportent aucune
#: information et allongent le vecteur de features pour rien.
SELECTED_SENSORS = [
    "sensor_2",
    "sensor_3",
    "sensor_4",
    "sensor_5",
    "sensor_6",
    "sensor_7",
    "sensor_8",
    "sensor_9",
    "sensor_11",
    "sensor_12",
    "sensor_13",
    "sensor_14",
    "sensor_15",
    "sensor_16",
    "sensor_17",
    "sensor_20",
    "sensor_21",
]


def load_rul_model(model_path: str = config.RUL_MODEL_PATH):
    """Charge le bundle modele et retourne (modele, colonnes de features)."""
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"Modele introuvable : {model_path}\n"
            "Executez notebooks/preprocess.ipynb pour l'entrainer et l'exporter."
        )

    bundle = joblib.load(model_path)
    return bundle["model"], bundle["feature_columns"]


def create_temporal_features(data: pd.DataFrame) -> pd.DataFrame:
    """Ajoute les features temporelles causales a l'historique d'un moteur.

    Pour chaque capteur on calcule :
    - la variation par rapport au cycle precedent (``_delta``) ;
    - l'ecart a la premiere observation observee (``_from_baseline``) ;
    - la moyenne et l'ecart-type glissants sur fenetres 5, 10 et 20.

    Les fenetres sont strictement causales : aucune statistique ne depend
    d'un cycle futur, ce qui evite toute fuite de donnees temporelle.

    Les 136 nouvelles colonnes sont assemblees dans un unique ``pd.concat``
    plutot qu'inserees une a une : en flux, l'historique est recalcule a chaque
    message, et l'insertion colonne par colonne degrade fortement les
    performances sur un DataFrame de plus de 100 colonnes.
    """
    ordered = data.sort_values(["unit_number", "time_in_cycles"]).reset_index(
        drop=True
    )

    derived: dict[str, pd.Series] = {}

    for sensor in SELECTED_SENSORS:

        grouped = ordered.groupby("unit_number", sort=False)[sensor]

        derived[f"{sensor}_delta"] = grouped.diff().fillna(0)

        derived[f"{sensor}_from_baseline"] = (
            ordered[sensor] - grouped.transform("first")
        )

        for window in config.ROLLING_WINDOWS:

            derived[f"{sensor}_mean_{window}"] = grouped.transform(
                lambda values: values.rolling(
                    window=window,
                    min_periods=1,
                ).mean()
            )

            derived[f"{sensor}_std_{window}"] = grouped.transform(
                lambda values: values.rolling(
                    window=window,
                    min_periods=2,
                ).std()
            ).fillna(0)

    return pd.concat([ordered, pd.DataFrame(derived, index=ordered.index)], axis=1)


def classify_rul(rul: float) -> str:
    """Traduit une RUL predite en decision de maintenance."""
    if rul <= 20:
        return "Critique"
    if rul <= 25:
        return "Surveillance renforcee"
    if rul <= 50:
        return "Inspection planifiee"
    return "Normal"


def main() -> None:
    """Consomme les mesures, predit la RUL et publie les alertes."""
    rul_model, feature_columns = load_rul_model()

    print(f"Modele charge   : {config.RUL_MODEL_PATH}")
    print(f"Features attendues : {len(feature_columns)}")

    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    consumer = KafkaConsumer(
        config.SENSOR_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=config.CONSUMER_GROUP_ID,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    engine_history: dict[int, list] = defaultdict(list)

    print(f"Consumer connecte a Kafka. Topic : {config.SENSOR_TOPIC}")
    print(f"Historique minimal requis : {config.MIN_HISTORY_CYCLES} cycles")
    print("Calcul des features en cours...\n")

    for message in consumer:

        data = message.value

        engine_id = int(data["unit_number"])
        cycle = int(data["time_in_cycles"])

        engine_history[engine_id].append(data)
        history = engine_history[engine_id]

        if len(history) < config.MIN_HISTORY_CYCLES:
            continue

        features_df = create_temporal_features(pd.DataFrame(history))

        missing_features = [
            feature
            for feature in feature_columns
            if feature not in features_df.columns
        ]

        if missing_features:
            print("Features manquantes :", missing_features)
            break

        latest = features_df.sort_values("time_in_cycles").tail(1)

        X_latest = (
            latest[feature_columns]
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0)
        )

        predicted_rul = max(0.0, float(rul_model.predict(X_latest)[0]))

        prediction_message = {
            "unit_number": engine_id,
            "cycle": cycle,
            "predicted_rul": round(predicted_rul, 2),
            "decision": classify_rul(predicted_rul),
            "history_size": len(history),
        }

        producer.send(config.RUL_TOPIC, value=prediction_message)
        producer.flush()

        print(
            f"Moteur {engine_id} | cycle {cycle} | "
            f"RUL predite : {predicted_rul:.2f} cycles | "
            f"{prediction_message['decision']}"
        )


if __name__ == "__main__":
    main()
