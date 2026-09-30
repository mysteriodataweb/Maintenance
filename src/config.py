"""Configuration centralisee du projet Maintenance.

Toutes les valeurs sensibles a l'environnement (chemins, adresse du broker,
topics) sont lues depuis les variables d'environnement avec une valeur par
defaut, afin que le meme code fonctionne sur une machine de developpement,
un poste de demonstration et un environnement de production.

Variables reconnues
-------------------
MAINTENANCE_ROOT
    Racine du depot. Par defaut, le dossier parent de ``src/``.
MAINTENANCE_DATASET_ID
    Sous-ensemble C-MAPSS utilise. Par defaut ``FD001``.
MAINTENANCE_RAW_DIR
    Dossier des donnees brutes. Par defaut ``<racine>/dataset/raw``.
MAINTENANCE_DATA_DIR
    Dossier des donnees traitees. Par defaut ``<racine>/data/processed``.
MAINTENANCE_MODEL_DIR
    Dossier des modeles serialises. Par defaut ``<racine>/models``.
MAINTENANCE_KAFKA_BOOTSTRAP
    Adresses du broker Kafka, separees par des virgules.
MAINTENANCE_SENSOR_TOPIC
    Topic d'entree des mesures capteurs.
MAINTENANCE_RUL_TOPIC
    Topic de sortie des predictions de RUL.
MAINTENANCE_CONSUMER_GROUP
    Groupe du consumer Kafka, pour la reprise apres redemarrage.
MAINTENANCE_MIN_HISTORY
    Nombre de cycles d'historique requis avant la premiere prediction.
MAINTENANCE_ALERT_HORIZON
    Horizon d'alerte en cycles utilise par le classifieur de panne.
MAINTENANCE_ALERT_THRESHOLD
    Seuil de probabilite au-dela duquel une alerte est emise.
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------

ROOT_DIR = os.path.abspath(
    os.environ.get(
        "MAINTENANCE_ROOT",
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
)

DATASET_ID = os.environ.get("MAINTENANCE_DATASET_ID", "FD001")

RAW_DATA_DIR = os.environ.get(
    "MAINTENANCE_RAW_DIR",
    os.path.join(ROOT_DIR, "dataset", "raw"),
)

PROCESSED_DATA_DIR = os.environ.get(
    "MAINTENANCE_DATA_DIR",
    os.path.join(ROOT_DIR, "data", "processed"),
)

MODEL_DIR = os.environ.get(
    "MAINTENANCE_MODEL_DIR",
    os.path.join(ROOT_DIR, "models"),
)

# ---------------------------------------------------------------------------
# Modeles
# ---------------------------------------------------------------------------

RUL_MODEL_PATH = os.path.join(
    MODEL_DIR,
    f"random_forest_{DATASET_ID}.joblib",
)

ALERT_MODEL_PATH = os.path.join(
    MODEL_DIR,
    f"random_forest_alert_{DATASET_ID}.joblib",
)

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "MAINTENANCE_KAFKA_BOOTSTRAP",
    "127.0.0.1:9092",
)

SENSOR_TOPIC = os.environ.get("MAINTENANCE_SENSOR_TOPIC", "sensor-data")

RUL_TOPIC = os.environ.get("MAINTENANCE_RUL_TOPIC", "rul-predictions")

CONSUMER_GROUP_ID = os.environ.get(
    "MAINTENANCE_CONSUMER_GROUP",
    f"rul-consumer-{DATASET_ID}",
)

#: Nombre de cycles d'historique accumules par moteur avant la premiere
#: prediction. Doit etre au moins egal a la plus grande fenetre glissante
#: (ROLLING_WINDOWS) pour que les features temporelles soient interpretables.
MIN_HISTORY_CYCLES = int(os.environ.get("MAINTENANCE_MIN_HISTORY", "20"))

# ---------------------------------------------------------------------------
# Cibles metier
# ---------------------------------------------------------------------------

#: Horizon d'alerte en cycles : RUL <= ALERT_HORIZON_CYCLES => panne imminente.
ALERT_HORIZON_CYCLES = int(os.environ.get("MAINTENANCE_ALERT_HORIZON", "30"))

#: Seuil de probabilite du classifieur d'alerte au-dela duquel on emet
#: une alerte maintenance.
ALERT_PROBABILITY_THRESHOLD = float(
    os.environ.get("MAINTENANCE_ALERT_THRESHOLD", "0.5")
)

# ---------------------------------------------------------------------------
# Ingenierie des caracteristiques
# ---------------------------------------------------------------------------

#: Fenetres glissantes causales utilisees pour chaque capteur.
ROLLING_WINDOWS = (5, 10, 20)

# ---------------------------------------------------------------------------
# Nommage des colonnes C-MAPSS
# ---------------------------------------------------------------------------

INDEX_COLUMNS = ["unit_number", "time_in_cycles"]

SETTING_COLUMNS = ["setting_1", "setting_2", "setting_3"]

SENSOR_COLUMNS = [f"sensor_{index}" for index in range(1, 22)]

#: Schema complet des 26 colonnes brutes de C-MAPSS.
COLUMN_NAMES = INDEX_COLUMNS + SETTING_COLUMNS + SENSOR_COLUMNS
