# Jeu de données C-MAPSS

Répartition, format et dictionnaire des colonnes du jeu de données de la NASA
utilisé par le projet.

---

## 1. Provenance

**C-MAPSS** (*Commercial Modular Aero-Propulsion System Simulation*),
Prognostics Center of Excellence, NASA Ames Research Center.

Il simule la dégradation d'un moteur à turbopropulseur composé de trois modules
principaux — **fan**, **core** (compresseur + chambre de combustion +
turbine), **LPC** (compresseur de basse pression) — sous conditions
d'exploitation et de défaillance variées.

Le jeu est censé reproduire les trois exigences d'un problème de maintenance
réelle :

- **Dégradation progressive**, non brutale et non linéaire.
- **Variabilité inter-parc**, des moteurs identiques se dégradient à des
  vitesses différentes, sans cause observable unique.
- **Rareté des données de panne**, un moteur de chaque série atteint son état
  terminal, la plupart n'ont pas de contrepartie.

Le téléchargement est assuré par `python -m src.download` (voir
[STREAMING.md](STREAMING.md) pour le détail des miroirs).

---

## 2. Les quatre sous-ensembles

| Sous-ensemble | Moteurs | Régimes de fonctionnement | Conditions |
| --- | --- | --- | --- |
| **FD001** | 100 | 1 | Altitude et vitesse constantes |
| FD002 | 260 | 6 | Six couples altitude/vitesse |
| FD003 | 100 | 1 | Altitude et vitesse constantes, 1/fan |
| FD004 | 249 | 6 | Six couples altitude/vitesse, 2 fans |

**Le projet cible FD001.** C'est le sous-ensemble le plus simple : un seul
régime, donc les réglages `setting_1..3` sont constants et les quatre
capteurs à variance nulle sont identifiés sans ambiguïté.

Les autres sous-ensembles ne sont pas interchangeables :

- **FD002 et FD004** combinent six régimes. Les réglages ne sont plus
  constants, et les capteurs constants de FD001 varient. La sélection de
  capteurs doit être recalculée, pas recopiée.
- **FD004** possède deux fans, donc deux fois plus de capteurs, et sa structure
  de sensors n'est pas compatible telle quelle avec FD001.

Passer à un autre sous-ensemble se fait par
`MAINTENANCE_DATASET_ID=FD002`, à condition d'avoir réentraîné les modèles sur
ce sous-ensemble et recalculé la sélection de capteurs.

---

## 3. Fichiers

| Fichier | Contenu |
| --- | --- |
| `train_FD00X.txt` | Données d'entraînement, tous les cycles de tous les moteurs |
| `test_FD00X.txt` | Données de test, **uniquement le dernier cycle** de chaque moteur |
| `RUL_FD00X.txt` | RUL de référence pour le test, une ligne par moteur, dans l'ordre de `unit_number` |

### Pourquoi le test ne contient qu'un cycle

Le format officiel impose d'estimer la RUL à partir de la dernière observation
seulement, sans accès à l'historique. C'est la contrainte la plus proche du cas
réel : au moment de la décision, on dispose de l'historique accumulé, mais pas
de la panne à venir.

Le pipeline Kafka de ce dépôt ne respecte pas cette contrainte : il reçoit les
cycles un à un en flux et reconstruit son propre historique. C'est réaliste, et
c'est pourquoi les features temporelles y sont indispensables — sans elles, un
modèle ne voit qu'un instantané et ne peut rien anticiper.

---

## 4. Format des fichiers

Texte brut, **séparateur tabulation ou espace**, **aucune ligne d'en-tête**.

```
1	1	0	0	0	8.5834		11.0		9.1933	...
2	1	0	0	0	8.4952		11.0		9.1925	...
1	2	0	0	0	8.5259		11.0		9.1812	...
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
| 3 | `setting_1` | float | Réglage 1. Sur FD001 : constante. |
| 4 | `setting_2` | float | Réglage 2. Sur FD001 : constante. |
| 5 | `setting_3` | float | Réglage 3. Sur FD001 : constante. |

Sur FD002–FD004, ces trois colonnes encodent les six couples
altitude/vitesse. Elles deviennent alors informatives, et la modélisation
doit en tenir compte — notamment parce que la RUL dépend du régime.

### 5.3 Capteurs

21 capteurs, tous en `float`. Ils ne sont pas nommés dans le fichier d'origine ;
`COLUMN_NAMES` dans `src/config.py` leur attribue les noms `sensor_1` à
`sensor_21`, dans l'ordre.

Le jeu de données d'origine ne fournit pas d'unité physique ni de fiche
technique par capteur. Les noms ci-dessous sont des **libellés usuels** de la
communauté C-MAPSS, à titre indicatif uniquement :

| Capteur | Libellé usuel | Retenu sur FD001 |
| --- | --- | --- |
| `sensor_1` | Température d'entrée compresseur (fan) | Non — constante |
| `sensor_2` | Température d'entrée compresseur (core) | Oui |
| `sensor_3` | Température de sortie LPC | Oui |
| `sensor_4` | Pression LPC entrante | Oui |
| `sensor_5` | Pression LPC sortante | Oui |
| `sensor_6` | Pression HPC entrante | Oui |
| `sensor_7` | Pression HPC sortante | Oui |
| `sensor_8` | Pression sortie physique | Oui |
| `sensor_9` | Ratio de pression | Oui |
| `sensor_10` | Température HPC | Non — constante |
| `sensor_11` | Température sortie HPC | Oui |
| `sensor_12` | Ratio de température | Oui |
| `sensor_13` | Température LPC | Oui |
| `sensor_14` | Chaleur massique HPC | Oui |
| `sensor_15` | Débit HPC | Oui |
| `sensor_16` | Ratio de débit | Oui |
| `sensor_17` | Pression surround | Oui |
| `sensor_18` | Température surround | Non — constante |
| `sensor_19` | Température HPC surround | Non — constante |
| `sensor_20` | Ratio de pression (2e étage) | Oui |
| `sensor_21` | Température surround (2e étage) | Oui |

**17 capteurs retenus, 4 retirés.** Sur FD001, les capteurs 1, 10, 18 et 19 ont
une variance nulle et ne portent aucune information. Cette sélection est
spécifique à FD001.

---

## 6. Volume

Sous-ensemble FD001 :

| Fichier | Lignes | Taille |
| --- | --- | --- |
| `train_FD001.txt` | 20 615 | ≈ 3,5 Mo |
| `test_FD001.txt` | 100 | ≈ 0,1 Mo |
| `RUL_FD001.txt` | 100 | ≈ 0,5 Ko |

Soit 100 moteurs d'entraînement et 100 moteurs de test, tous de même
génération. Les quatre sous-ensembles réunis pèsent ≈ 43 Mo, raison pour
laquelle le répertoire `dataset/raw/` n'est pas versionné.

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

Pour `RUL_FD001.txt`, un moteur dont le dernier cycle observé est `T` suit une
dégradation linéaire, de `T - 1` à `0` : le cycle de panne n'est pas observé et
la dégradation est supposée totale au dernier relevé.

**Limite connue.** La RUL est ainsi parfaitement corrélée à `time_in_cycles` à
l'intérieur d'un moteur. Cette régularité ne se retrouve pas sur un parc réel,
où la dégradation présente du bruit et de la variabilité. Les résultats
obtenus sur C-MAPSS constituent un **plancher**, pas une prévision de
performance en production.

---

## 8. Répartition de la RUL de référence

`RUL_FD001.txt` ne contient qu'une valeur par moteur, de 7 à 145 cycles. Cette
distribution Guides l'interprétation des métriques : un modèle qui prédit
systématiquement autour de la moyenne de la distribution peu distinguishera les
moteurs authentiquement proches de la panne des autres.

C'est pourquoi l'évaluation ne se limite pas au score agrégé et examine
explicitement les faibles RUL, là où une erreur a des conséquences
opérationnelles. Voir [ARCHITECTURE.md](ARCHITECTURE.md), section 6.
