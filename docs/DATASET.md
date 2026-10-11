# Jeu de données C-MAPSS

Répartition, format et dictionnaire des colonnes du jeu de données de la NASA
utilisé par le projet.

---

## 1. Provenance

**C-MAPSS** (*Commercial Modular Aero-Propulsion System Simulation*),
Prognostics Center of Excellence, NASA Ames Research Center.

Il simule la dégradation d'un turboréacteur double flux (*turbofan*) composé
de cinq modules tournants — **soufflante** (fan), **compresseur basse
pression** (LPC), **compresseur haute pression** (HPC), **turbine haute
pression** (HPT) et **turbine basse pression** (LPT) — sous conditions
d'exploitation et de défaillance variées.

Le jeu reproduit trois caractéristiques d'un problème de maintenance réelle :

- **Dégradation progressive**, non brutale et non linéaire.
- **Variabilité inter-parc** : des moteurs identiques démarrent avec une usure
  initiale différente et se dégradent à des vitesses différentes.
- **Historiques tronqués** : en entraînement, chaque moteur est suivi jusqu'à
  la panne ; en test, la série s'arrête avant la panne et il faut estimer le
  nombre de cycles restants.

Le téléchargement est assuré par `python -m src.download` (les miroirs sont
listés dans `src/download.py`).

---

## 2. Les quatre sous-ensembles

| Sous-ensemble | Moteurs (train / test) | Régimes de fonctionnement | Modes de défaillance |
| --- | --- | --- | --- |
| **FD001** | 100 / 100 | 1 | 1 (HPC) |
| FD002 | 260 / 259 | 6 | 1 (HPC) |
| FD003 | 100 / 100 | 1 | 2 (HPC et soufflante) |
| FD004 | 249 / 248 | 6 | 2 (HPC et soufflante) |

**Le projet cible FD001.** C'est le sous-ensemble le plus simple : un seul
régime et un seul mode de défaillance. Les réglages `setting_1` et `setting_2`
n'y varient que par un bruit infime autour d'un point de fonctionnement
unique, et `setting_3` est constant.

Les quatre sous-ensembles ont le même schéma à 26 colonnes, mais ne sont pas
interchangeables :

- **FD002 et FD004** combinent six régimes. Les réglages deviennent
  informatifs, et les capteurs constants de FD001 varient avec le régime. La
  sélection de capteurs doit être recalculée, pas recopiée, et les mesures
  normalisées par régime.
- **FD003 et FD004** ajoutent un second mode de défaillance (soufflante) :
  les capteurs ne dérivent plus tous dans le même sens d'un moteur à l'autre.

Passer à un autre sous-ensemble se fait par
`MAINTENANCE_DATASET_ID=FD002`, à condition d'avoir réentraîné les modèles sur
ce sous-ensemble et recalculé la sélection de capteurs.

---

## 3. Fichiers

| Fichier | Contenu |
| --- | --- |
| `train_FD00X.txt` | Données d'entraînement, tous les cycles de chaque moteur **jusqu'à la panne** |
| `test_FD00X.txt` | Données de test, tous les cycles de chaque moteur jusqu'à un point d'arrêt **antérieur à la panne** |
| `RUL_FD00X.txt` | RUL de référence au dernier cycle de test, une ligne par moteur, dans l'ordre de `unit_number` |

### Ce que contient le test

Le fichier de test fournit l'historique complet de chaque moteur jusqu'à son
dernier cycle observé (de 31 à 303 cycles sur FD001). L'évaluation officielle
porte sur **une seule prédiction par moteur**, celle du dernier cycle, comparée
à `RUL_FD00X.txt`. C'est la situation réelle : au moment de la décision, on
dispose de l'historique accumulé, mais pas de la panne à venir.

Le pipeline Kafka rejoue ce fichier cycle par cycle et publie une prédiction à
chaque cycle. Seule la dernière prédiction de chaque moteur est comparable à
la référence ; les précédentes n'ont pas de vérité terrain dans le jeu de
test.

---

## 4. Format des fichiers

Texte brut, **colonnes séparées par des espaces**, **aucune ligne d'en-tête**.

```
1 1 -0.0007 -0.0004 100.0 518.67 641.82 1589.70 1400.60 14.62 21.61 554.36 ...
1 2 0.0019 -0.0003 100.0 518.67 642.15 1591.82 1403.14 14.62 21.61 553.75 ...
```

Lecture :

```python
pd.read_csv(
    "train_FD001.txt",
    sep=r"\s+",
    header=None,
    names=COLUMN_NAMES,
)
```

---

## 5. Dictionnaire des colonnes

26 colonnes, dans cet ordre exact. L'ordre est significatif : le fichier n'a
pas d'en-tête, et un décalage fausserait silencieusement l'apprentissage.

### 5.1 Identifiants

| # | Nom | Type | Description |
| --- | --- | --- | --- |
| 1 | `unit_number` | int | Identifiant unique du moteur. Clé de groupement, **jamais une feature**. |
| 2 | `time_in_cycles` | int | Nombre de cycles écoulés depuis le début de la mesure. |

### 5.2 Réglages opérationnels

| # | Nom | Type | Description |
| --- | --- | --- | --- |
| 3 | `setting_1` | float | Altitude. Sur FD001 : bruit entre −0,0087 et +0,0087. |
| 4 | `setting_2` | float | Nombre de Mach. Sur FD001 : bruit entre −0,0006 et +0,0006. |
| 5 | `setting_3` | float | Angle de la manette des gaz (TRA). Sur FD001 : constant à 100. |

Sur FD001, `setting_1` et `setting_2` varient donc très légèrement : le
notebook les conserve comme features, `setting_3` est écarté. Sur FD002 et
FD004, ces trois colonnes encodent six régimes de vol. Elles deviennent alors
réellement informatives, et la modélisation doit en tenir compte — les
capteurs changent de niveau d'un régime à l'autre.

### 5.3 Capteurs

21 capteurs. Ils ne sont pas nommés dans le fichier d'origine ; `COLUMN_NAMES`
dans `src/config.py` leur attribue les noms `sensor_1` à `sensor_21`, dans
l'ordre. Les libellés et unités ci-dessous sont ceux de la publication NASA
(*Damage Propagation Modeling*, Saxena et al., 2008), fournie avec l'archive :

| Capteur | Symbole | Grandeur | Unité | Retenu sur FD001 |
| --- | --- | --- | --- | --- |
| `sensor_1` | T2 | Température totale à l'entrée de la soufflante | °R | Non — constant |
| `sensor_2` | T24 | Température totale en sortie du LPC | °R | Oui |
| `sensor_3` | T30 | Température totale en sortie du HPC | °R | Oui |
| `sensor_4` | T50 | Température totale en sortie de la LPT | °R | Oui |
| `sensor_5` | P2 | Pression à l'entrée de la soufflante | psia | Oui (quasi constant) |
| `sensor_6` | P15 | Pression totale dans le canal secondaire | psia | Oui |
| `sensor_7` | P30 | Pression totale en sortie du HPC | psia | Oui |
| `sensor_8` | Nf | Vitesse physique de la soufflante | tr/min | Oui |
| `sensor_9` | Nc | Vitesse physique du corps haute pression | tr/min | Oui |
| `sensor_10` | epr | Rapport de pression moteur (P50/P2) | — | Non — constant |
| `sensor_11` | Ps30 | Pression statique en sortie du HPC | psia | Oui |
| `sensor_12` | phi | Débit carburant rapporté à Ps30 | pps/psi | Oui |
| `sensor_13` | NRf | Vitesse corrigée de la soufflante | tr/min | Oui |
| `sensor_14` | NRc | Vitesse corrigée du corps haute pression | tr/min | Oui |
| `sensor_15` | BPR | Taux de dilution | — | Oui |
| `sensor_16` | farB | Richesse carburant/air de la chambre | — | Oui (quasi constant) |
| `sensor_17` | htBleed | Enthalpie de prélèvement | — | Oui |
| `sensor_18` | Nf_dmd | Vitesse de soufflante demandée | tr/min | Non — constant |
| `sensor_19` | PCNfR_dmd | Vitesse corrigée de soufflante demandée | tr/min | Non — constant |
| `sensor_20` | W31 | Débit d'air de refroidissement HPT | lbm/s | Oui |
| `sensor_21` | W32 | Débit d'air de refroidissement LPT | lbm/s | Oui |

**17 capteurs retenus, 4 retirés.** Sur FD001, les capteurs 1, 10, 18 et 19 ont
une variance strictement nulle et sont retirés par le notebook. Cette sélection
est spécifique à FD001.

Les capteurs 5 et 16 ne prennent eux aussi qu'une seule valeur sur FD001, mais
leur variance calculée vaut 1e-29 au lieu de 0 à cause des arrondis en virgule
flottante : le test `variance == 0` les laisse passer. Ils sont donc conservés
et génèrent 16 features sans information. Un test sur le nombre de valeurs
distinctes les éliminerait, au prix d'un changement de la liste de features
dans le notebook **et** dans `streaming/consumer.py`.

---

## 6. Volume

Sous-ensemble FD001 :

| Fichier | Lignes | Taille |
| --- | --- | --- |
| `train_FD001.txt` | 20 631 | ≈ 3,5 Mo |
| `test_FD001.txt` | 13 096 | ≈ 2,2 Mo |
| `RUL_FD001.txt` | 100 | ≈ 0,4 Ko |

Soit 100 moteurs d'entraînement (128 à 362 cycles chacun) et 100 moteurs de
test (31 à 303 cycles). Les quatre sous-ensembles réunis pèsent ≈ 43 Mo, raison
pour laquelle le répertoire `dataset/raw/` n'est pas versionné.

---

## 7. Cible : la RUL

Non fournie dans les fichiers d'entraînement, elle se déduit de la durée de vie
maximale de chaque moteur :

```python
df["RUL"] = (
    df.groupby("unit_number")["time_in_cycles"].transform("max")
    - df["time_in_cycles"]
)
```

Un moteur d'entraînement dont le dernier cycle est `T` a une RUL qui décroît
linéairement de `T - 1` à `0`. Pour l'entraînement, cette cible est
**plafonnée à 125 cycles** : en début de vie les capteurs ne portent aucun
signe de dégradation, et 38,9 % des lignes ont une RUL supérieure à 125 que
rien ne permet de distinguer. Voir [ARCHITECTURE.md](ARCHITECTURE.md),
section 3.

**Limite connue.** C-MAPSS est une simulation : la dégradation y est régulière
et tous les moteurs vont jusqu'à la panne. Sur un parc réel, les moteurs sont
déposés avant la panne, les capteurs dérivent et les régimes changent. Les
résultats obtenus ici ne sont pas une prévision de performance en production.

---

## 8. Répartition de la RUL de référence

`RUL_FD001.txt` contient une valeur par moteur de test, de 7 à 145 cycles
(moyenne 75,5 ; 11 moteurs au-dessus de 125). Cette distribution guide
l'interprétation des métriques : un modèle qui prédit systématiquement autour
de la moyenne obtient un score agrégé acceptable sans distinguer les moteurs
réellement proches de la panne.

C'est pourquoi l'évaluation ne se limite pas au score agrégé et examine
explicitement les faibles RUL, là où une erreur a des conséquences
opérationnelles. Voir [ARCHITECTURE.md](ARCHITECTURE.md), section 6.
