# Pipeline Kafka

Contrat des topics, cycle de vie des messages, reprise et mise en production.

---

## 1. Rôle

Le pipeline Kafka porte la dimension temps réel du projet : là où le notebook
produit une prédiction par lot, le consumer en produit une par cycle, à mesure
que les mesures arrivent.

C'est ce qui distingue le projet d'un simple exercice de régression : le modèle
est ici au service d'un flux, avec des contraintes que le notebook n'a pas —
ordre des messages, état à maintenir entre deux messages, capacité à tolérer un
redémarrage.

---

## 2. Topics

| Topic | Sens | Producteur | Consommateurs |
| --- | --- | --- | --- |
| `sensor-data` | Entrée | `streaming/producer.py` | `streaming/consumer.py` |
| `rul-predictions` | Sortie | `streaming/consumer.py` | GMAO, tableaux de bord, CMMS |

Noms configurables via `MAINTENANCE_SENSOR_TOPIC` et
`MAINTENANCE_RUL_TOPIC`.

---

## 3. Topic `sensor-data`

Une mesure par cycle moteur, encodée en JSON.

### Clé et partitionnement

Le producteur utilise `producer.send(topic, value=...)` sans clé. Les messages
d'un même moteur peuvent donc atterrir sur des partitions différentes.

**Ce n'est pas un problème ici**, parce que l'historique est reconstruit par
`unit_number` au sein du consumer, pas par partition. Mais si le consumer
évolue vers un état incrémental réparti entre plusieurs instances, la clé
deviendra obligatoire :

```python
producer.send(config.SENSOR_TOPIC, key=str(engine_id).encode(), value=message)
```

Cela garantirait que tous les cycles d'un moteur vont au même consumer, donc
que son état incrémental reste cohérent.

### Payload

```json
{
  "unit_number": 7,
  "time_in_cycles": 143,
  "setting_1": 0.0,
  "setting_2": 0.0,
  "setting_3": 0.0,
  "sensor_1": 9.4836,
  "sensor_2": 11.32,
  "sensor_3": 6.4771,
  "...": "..."
}
```

Les 26 colonnes de C-MAPSS, noms standardisés par `src/config.py`. Chaque
valeur est un nombre ; le désérialiseur fait un `json.loads` direct.

---

## 4. Topic `rul-predictions`

Une prédiction par moteur et par cycle traité.

### Payload

```json
{
  "unit_number": 7,
  "cycle": 143,
  "predicted_rul": 18.42,
  "decision": "Critique",
  "history_size": 143
}
```

| Champ | Type | Rôle |
| --- | --- | --- |
| `unit_number` | int | Moteur concerné |
| `cycle` | int | Cycle d'origine de la prédiction |
| `predicted_rul` | float | RUL prédite, arrondie à 2 décimales, jamais négative |
| `decision` | string | Décision de maintenance déduite des seuils |
| `history_size` | int | Nombre de cycles accumulés pour ce moteur |

### Décision

| RUL prédite | Décision |
| --- | --- |
| ≤ 20 | `Critique` |
| 21 – 25 | `Surveillance renforcee` |
| 26 – 50 | `Inspection planifiee` |
| > 50 | `Normal` |

Les accents ont été retirés des libellés pour éviter tout problème
d'encodage entre producteurs, broker et consommateurs.

**Ces seuils pilotent une décision d'intervention.** Les faire évoluer est un
arbitrage métier, pas un réglage technique : chaque seuil modifie le nombre
d'interventions déclenchées, donc la charge de la GMAO. Toute modification doit
être évaluée sur la matrice de confusion métier du notebook, en portant une
attention particulière aux faux négatifs de la classe `Critique`.

---

## 5. Cycle de vie du consumer

```
message reçu
    │
    ├──► engine_history[unit_number].append(message)
    │
    ├──► len(history) < MIN_HISTORY_CYCLES ?  ──► oui ──► continuer
    │                                        non
    ├──► create_temporal_features(historique)   136 features
    │
    ├──► une feature attendue est absante ?    ──► oui ──► journaliser et arrêter
    │                                        non
    ├──► sélection du dernier cycle
    ├──► prediction du modèle
    ├──► publication sur "rul-predictions"
    └──► boucle
```

### Les 20 premiers cycles

Aucune prédiction n'est émise tant qu'un moteur n'a pas
`MAINTENANCE_MIN_HISTORY` cycles (20 par défaut) d'historique. Ce seuil est
posé par la plus grande fenêtre glissante : en dessous, les features `_std_20`
seraient calculées sur une fenêtre partielle et le modèle recevrait une
entrée qui n'a jamais existé à l'entraînement.

`min_periods=1` sur les moyennes et `min_periods=2` sur les écarts-types
autorisent le calcul sur des historiques courts, mais cela ne garantit pas que
la distribution des features ressemble à celle vue à l'entraînement. Le seuil
de 20 cycles est cette garantie minimale.

---

## 6. Reprise et idempotence

### État en mémoire

`engine_history` est un `defaultdict(list)` en mémoire vive. Deux conséquences :

- **Au redémarrage, tout est perdu.** Chaque moteur doit réaccumuler 20 cycles
  avant sa première prédiction. Pour un déploiement durable, brancher un store
  de séries temporelles (Redis, InfluxDB, TimescaleDB).
- **Le consumer n'est pas partageable.** Deux instances lisant le même
  `group_id` se partagent les partitions, donc chaque moteur est traité par une
  seule instance — c'est correct. Mais deux consumer sur des `group_id`
  différents traiteraient chacun l'intégralité du flux, et produiraient des
  prédictions contradictoires sur `rul-predictions`.

### Offsets

```python
auto_offset_reset="earliest"
enable_auto_commit=True
group_id=config.CONSUMER_GROUP_ID
```

`auto_offset_reset="earliest"` ne s'applique qu'aux groupes **sans** offset
enregistré : la première exécution avec un nouveau `group_id` relit donc tout
l'historique du topic. Ensuite, l'auto-commit reprend où le consumer s'était
arrêté.

Changer `MAINTENANCE_CONSUMER_GROUP` provoque un rejeu complet et une rafale de
prédictions sur des données déjà traitées. En choisir un nouveau, vider d'abord
le topic.

**Limite à connaître :** l'offset est commité après le traitement, jamais avant.
Si le consumer tombe entre la prédiction et le commit, le message est rejoué et
la prédiction republiée. Les consommateurs aval doivent donc être **idempotents**,
par exemple sur la clé `(unit_number, cycle)`.

---

## 7. Mise en production

Ce qui manque pour un déploiement réel, par ordre de priorité :

| Manque | Impact | Correctif attendu |
| --- | --- | --- |
| Historique non persisté | Perte d'état à chaque redémarrage | Store de séries temporelles |
| Un seul consumer | Pas de montée en charge | Clé de partitionnement par `unit_number` |
| Features recalculées en entier | Coût linéaire en historique | État incrémental par moteur |
| Pas de supervision | Panne silencieuse | Métriques de lag, de débit, de RUL prédite |
| Pas de suivi de dérive | Prédictions dégradées silencieuses | Alerte sur l'écart des features à la distribution d'entraînement |
| Modèle non versionné avec le code | Déploiement non reproductible | Registre d'artefacts (MLflow, bucket objet, Git LFS) |
| `producer.flush()` par message | Débit limité | Accumulation par lot, `linger_ms` ajusté |

### Supervision

Trois indicateurs à instrumenter en priorité :

- **Lag du consumer** sur `sensor-data` : au-delà de quelques secondes, la
  prédiction n'est plus exploitable pour une décision d'intervention.
- **Débit de messages sur `rul-predictions`** : une chute à zéro alors que le
  consumer tourne signale un modèle qui ne prédit plus — par exemple une
  dérive des données produisant des `NaN`.
- **Distribution des RUL prédites** : un déplacement brutal vers les faibles
  valeurs, ou vers les hautes, trahit une dérive capteur bien avant que la
  performance du modèle ne se dégrade visiblement.
