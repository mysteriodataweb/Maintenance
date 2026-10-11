# Maintenance — Maintenance prédictive par streaming

Prédiction de la **durée de vie restante** (*Remaining Useful Life*, RUL) de
turboréacteurs double flux à partir de mesures capteurs, avec un pipeline Kafka
qui ingère les mesures, calcule les features temporelles et publie une décision
de maintenance par moteur.

Le projet s'appuie sur le jeu de données **C-MAPSS** de la NASA (données de
dégradation de moteurs, 4 sous-ensembles, 26 mesures par cycle).

---

## Sommaire

- [Objectif](#objectif)
- [Architecture](#architecture)
- [Décisions métier](#décisions-métier)
- [Installation](#installation)
- [Prise en main](#prise-en-main)
- [Entraînement des modèles](#entraînement-des-modèles)
- [Exécution du pipeline Kafka](#exécution-du-pipeline-kafka)
- [Configuration](#configuration)
- [Structure du dépôt](#structure-du-dépôt)
- [Données non versionnées](#données-non-versionnées)
- [Documentation complémentaire](#documentation-complémentaire)

---

## Objectif

Pour un moteur donné et à un cycle donné, estimer le nombre de cycles
restants avant défaillance permet de planifier une intervention **avant** la
panne, plutôt que de la subir.

Le projet couvre deux besoins distincts :

| Besoin | Réalisation |
| --- | --- |
| **Explorer et entraîner** | `notebooks/preprocess.ipynb` : nettoyage, ingénierie de features, validation, évaluation |
| **Servir la prédiction** | `streaming/` : producton Kafka → consumer → prédiction → republication |

---

## Architecture

```
   C-MAPSS (test_FD001.txt)
              │
              │  streaming/producer.py
              ▼
   ┌──────────────────────┐
   │  topic "sensor-data" │   26 colonnes par message, JSON
   └──────────┬───────────┘
              │
              │  streaming/consumer.py
              │   • historique par moteur (≥ 20 cycles)
              │   • features temporelles causales
              │   • Random Forest de régression
              ▼
   ┌──────────────────────────┐
   │  topic "rul-predictions" │   unit_number, cycle, predicted_rul, decision
   └──────────────────────────┘
```

Le consumer **recalcule** l'intégralité des features à chaque message reçu pour
un moteur. Cette approche est volontairement simple et correcte, mais elle est
coûteuse en calcul : le passage à un état incrémental par moteur est la
principale piste d'optimisation (voir [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)).

---

## Décisions métier

Une RUL prédite est traduite en décision de maintenance :

| RUL prédite (cycles) | Décision | Action |
| --- | --- | --- |
| ≤ 20 | **Critique** | Intervention immédiate, retirer le moteur de service |
| 21 – 25 | **Surveillance renforcée** | Suivre les capteurs du moteur au cycle |
| 26 – 50 | **Inspection planifiée** | Programmer la maintenance dans le cycle de vie |
| > 50 | **Normal** | Aucune action |

Le score de la NASA, utilisé pour l'évaluation, est **asymétrique** : une
surestimation de la RUL est bien plus pénalisée qu'une sous-estimation, car
surestimer revient à retarder l'intervention sur un moteur qui va tomber.
Les seuils ci-dessus sont alignés sur cette logique.

---

## Installation

Prérequis : **Python 3.10+**. Kafka n'est nécessaire que pour la partie
streaming.

```bash
git clone https://github.com/mysteriodataweb/Maintenance.git
cd Maintenance

python -m venv .venv
```

Activation :

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

```bash
# Linux / macOS
source .venv/bin/activate
```

Puis les dépendances :

```bash
pip install -r requirements.txt
```

---

## Prise en main

### 1. Télécharger le jeu de données

Le script récupère l'archive C-MAPSS officielle et l'extrait dans
`dataset/raw/`. Il est idempotent : relancer ne retélécharge rien.

```bash
python -m src.download
```

Les données ne sont pas versionnées (≈ 43 Mo). Voir
[Données non versionnées](#données-non-versionnées).

### 2. Explorer et entraîner

```bash
jupyter lab
```

Ouvrir `notebooks/preprocess.ipynb` et exécuter les cellules dans l'ordre. Le
notebook :

1. charge et documente les 26 colonnes C-MAPSS ;
2. contrôle la qualité des données et retire les capteurs constants ;
3. calcule la RUL cible et les features temporelles ;
4. entraîne et compare plusieurs régresseurs, avec validation par
   `GroupShuffleSplit` (aucun moteur n'apparaît à la fois en entraînement et
   en validation) ;
5. évalue sur le test officiel avec le score NASA et la matrice de confusion
   métier ;
6. exporte les modèles dans `models/`.

> Le notebook lit `../dataset/raw` et écrit `../data/processed` : il attend d'être
> lancé depuis le dossier `notebooks/`, ce qui est le comportement par défaut
> de Jupyter Lab.

---

## Entraînement des modèles

L'entraînement passe par le notebook, qui produit deux fichiers dans `models/` :

| Fichier | Rôle |
| --- | --- |
| `random_forest_FD001.joblib` | Régression de la RUL |
| `random_forest_alert_FD001.joblib` | Classification « panne imminente » (RUL ≤ 30 cycles) |

Chaque fichier est un dictionnaire contenant le modèle et la liste **ordonnée**
de ses features. Cette liste est indispensable : l'ordre des colonnes doit être
identique à l'entraînement, faute de quoi la prédiction est silencieusement
faux.

Vérifier les deux bundles et la cohérence de leurs features :

```bash
python -m streaming.load_models
```

---

## Exécution du pipeline Kafka

### Prérequis

Un broker Kafka accessible. L'installation la plus simple, via Docker :

```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  bitnami/kafka:latest
```

Créer les deux topics :

```bash
docker exec kafka kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic sensor-data --partitions 1 --replication-factor 1

docker exec kafka kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic rul-predictions --partitions 1 --replication-factor 1
```

### Lancer les deux processus

Dans deux terminaux, environnement virtuel activé.

**Terminal 1 — le consumer** (à démarrer en premier, il charge le modèle) :

```bash
python -m streaming.consumer
```

**Terminal 2 — le producer** :

```bash
python -m streaming.producer
```

### Lire les prédictions

```bash
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic rul-predictions --from-beginning
```

Chaque message publié :

```json
{
  "unit_number": 7,
  "cycle": 143,
  "predicted_rul": 18.42,
  "decision": "Critique",
  "history_size": 143
}
```

---

## Configuration

Toute la configuration vit dans [`src/config.py`](src/config.py) et se surcharge
par variable d'environnement. Aucun chemin absolu n'est codé en dur.

| Variable | Défaut | Rôle |
| --- | --- | --- |
| `MAINTENANCE_ROOT` | racine du dépôt | Racine du projet |
| `MAINTENANCE_DATASET_ID` | `FD001` | Sous-ensemble C-MAPSS |
| `MAINTENANCE_RAW_DIR` | `<racine>/dataset/raw` | Données brutes |
| `MAINTENANCE_DATA_DIR` | `<racine>/data/processed` | Données traitées |
| `MAINTENANCE_MODEL_DIR` | `<racine>/models` | Modèles sérialisés |
| `MAINTENANCE_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Adresses du broker |
| `MAINTENANCE_SENSOR_TOPIC` | `sensor-data` | Topic d'entrée |
| `MAINTENANCE_RUL_TOPIC` | `rul-predictions` | Topic de sortie |
| `MAINTENANCE_CONSUMER_GROUP` | `rul-consumer-<ID>` | Groupe du consumer |
| `MAINTENANCE_MIN_HISTORY` | `20` | Cycles requis avant prédiction |
| `MAINTENANCE_ALERT_HORIZON` | `30` | Horizon d'alerte, en cycles |
| `MAINTENANCE_ALERT_THRESHOLD` | `0.5` | Seuil de probabilité d'alerte |
| `MAINTENANCE_PRODUCER_INTERVAL` | `0.1` | Délai entre messages (s) |

Exemple sous Windows PowerShell :

```powershell
$env:MAINTENANCE_KAFKA_BOOTSTRAP = "broker-1:9092,broker-2:9092"
$env:MAINTENANCE_MIN_HISTORY = "50"
python -m streaming.consumer
```

---

## Structure du dépôt

```
Maintenance/
├── README.md
├── requirements.txt
├── .gitignore
├── docs/
│   ├── ARCHITECTURE.md          Détail du pipeline et de la feature engineering
│   ├── DATASET.md               Format C-MAPSS et dictionnaire des colonnes
│   └── STREAMING.md             Contrat Kafka et exploitation du flux
├── src/
│   ├── __init__.py
│   ├── config.py                Configuration centralisée
│   └── download.py              Téléchargement C-MAPSS
├── streaming/
│   ├── __init__.py
│   ├── producer.py              Publication des mesures
│   ├── consumer.py              Prédiction de la RUL en flux
│   └── load_models.py           Diagnostic des bundles sérialisés
├── notebooks/
│   └── preprocess.ipynb         Exploration, entraînement, évaluation
├── dataset/raw/                 Données C-MAPSS (non versionnées)
├── data/processed/              CSV enrichis (non versionnés)
└── models/                      Modèles .joblib (non versionnés)
```

> Le dossier de code s'appelle `streaming/` et non `kafka/` : un package local
> du même nom que la librairie cliente `kafka-python` masquerait ses imports et
> casserait `python -m streaming.consumer`.

---

## Données non versionnées

Trois répertoires sont ignorés par git :

| Répertoire | Contenu | Régénération |
| --- | --- | --- |
| `dataset/raw/` | ≈ 43 Mo, fichiers `.txt` C-MAPSS | `python -m src.download` |
| `data/processed/` | ≈ 12 Mo, CSV enrichis | `notebooks/preprocess.ipynb` |
| `models/` | ≈ 247 Mo, `.joblib` | `notebooks/preprocess.ipynb` |

`random_forest_FD001.joblib` pèse à lui seul ≈ **233 Mo** : il dépasse la limite
de 100 Mo par fichier de GitHub et ne peut pas être versionné. Il s'agit d'un
artefact de ré-entraînement, pas de source : le notebook qui le produit est
versionné, la procédure pour le reconstruire est décrite, et c'est cette
reproductibilité qui est versionnée.

Les gros modèles sont à conserver dans un registre d'artefacts (MLflow, un
bucket objet, ou des Git LFS) si l'on a besoin de les partager.

---

## Documentation complémentaire

| Document | Contenu |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Pipeline, ingénierie de features, validation, pistes d'optimisation |
| [docs/DATASET.md](docs/DATASET.md) | Répartition C-MAPSS, dictionnaire des 26 colonnes, capteurs retenus |
| [docs/STREAMING.md](docs/STREAMING.md) | Contrat des topics Kafka, payloads, reprise, mise en production |

---

## Licence et provenance des données

Le code de ce dépôt est fourni tel quel, à des fins de démonstration technique.

C-MAPSS est un jeu de données public de la NASA (*Prognostics Center of
Excellence*, NASA Ames Research Center). Il est redistribuable ; `src/download.py`
le récupère depuis des miroirs publics.
