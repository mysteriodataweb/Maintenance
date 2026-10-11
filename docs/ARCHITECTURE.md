# Architecture

Détail du pipeline, de la construction des features et des choix de validation.

---

## 1. Vue d'ensemble

Le projet comporte deux chemins distincts, qui partagent la même fonction
d'ingénierie de features.

### Chemin hors ligne — recherche et entraînement

```
dataset/raw/train_FD001.txt
        │
        ▼
notebooks/preprocess.ipynb
  • charge, contrôle, nettoie
  • calcule la RUL cible
  • engineered features
  • GroupShuffleSplit (validation par moteurs)
  • comparaison de modèles
  • test officiel + score NASA
        │
        ▼
models/*.joblib  +  data/processed/*.csv
```

### Chemin en ligne — service de prédiction

```
dataset/raw/test_FD001.txt
        │
        ▼
streaming/producer.py
        │
        ▼
   topic "sensor-data"  ──────────►  streaming/consumer.py
                                              │
                                              ▼
                                    topic "rul-predictions"
```

**Point de vigilance.** La fonction `create_temporal_features` existe à deux
endroits : dans le notebook et dans `streaming/consumer.py`. Les deux versions
doivent produire exactement les mêmes colonnes, dans le même ordre, sinon le
modèle reçoit un vecteur incohérent. En cas de modification de l'une, mettre à
jour l'autre et relancer `python -m streaming.load_models` pour contrôler la
concordance des listes de features.

---

## 2. Données d'entrée

Le jeu C-MAPSS fournit 26 colonnes par cycle. Voir [DATASET.md](DATASET.md) pour
le dictionnaire complet.

| Catégorie | Colonnes | Traitement |
| --- | --- | --- |
| Identifiants | `unit_number`, `time_in_cycles` | `unit_number` sert au groupement, jamais comme feature |
| Réglages | `setting_1..3` | Conservés s'ils varient : sur FD001, `setting_1` et `setting_2` (bruit infime), `setting_3` constant écarté |
| Capteurs | `sensor_1..21` | 17 retenus, 4 constants retirés |

### Capteurs retirés

Sur FD001, les capteurs **1, 10, 18 et 19** ont une variance nulle : ils ne
portent aucune information et ne feraient qu'allonger le vecteur de features.

**Avertissement :** cette sélection est propre à FD001. Les sous-ensembles
FD002 à FD004 contiennent plusieurs régimes de fonctionnement et n'ont pas les
mêmes capteurs constants. Changer de sous-ensemble impose de recalculer la
sélection, pas de recopier celle de FD001.

---

## 3. Cible

La RUL est calculée sur les données d'entraînement :

```python
df["RUL"] = (
    df.groupby("unit_number")["time_in_cycles"].transform("max")
    - df["time_in_cycles"]
)
```

Un moteur dont le dernier cycle observé est `T` a donc une RUL qui décroît
linéairement de `T - 1` à `0` : en entraînement, chaque moteur est suivi
jusqu'à la panne, et son dernier relevé porte la RUL `0`.

### Plafonnement de la cible

Les modèles sont entraînés sur la RUL **plafonnée à 125 cycles** :

```python
RUL_CAP = 125
df["RUL_capped"] = df["RUL"].clip(upper=RUL_CAP)
```

En début de vie, les capteurs ne montrent aucune dégradation : un moteur à 300
cycles de la panne et un moteur à 150 cycles ont les mêmes mesures. Sans
plafond, 38,9 % des lignes d'entraînement portent une RUL supérieure à 125 que
rien ne permet de prédire, et le modèle apprend à surestimer. Avec le plafond,
il apprend « moteur sain » au-delà de 125 cycles et se concentre sur la phase
de dégradation, la seule qui pilote une décision.

Conséquence : le modèle ne prédit jamais plus de 125 cycles. C'est sans effet
sur les décisions, dont le seuil le plus haut est à 50 cycles.

### Jeu de test

Le fichier de test officiel contient l'historique de chaque moteur jusqu'à un
cycle antérieur à la panne. L'évaluation porte sur la prédiction au **dernier
cycle observé** de chaque moteur. Les cibles de référence sont dans
`RUL_FD001.txt`, une RUL par moteur, dans l'ordre des `unit_number`.

---

## 4. Ingénierie des features

Pour chacun des 17 capteurs retenus, huit colonnes sont produites, soit
**136 features dérivées**. S'y ajoutent les 17 mesures brutes et les 2 réglages
variables (`setting_1`, `setting_2`), pour **155 entrées** dans le vecteur
final. Les identifiants n'en font pas partie.

| Suffixe | Définition | Fenêtre |
| --- | --- | --- |
| `_delta` | `x(t) - x(t-1)` | 1 cycle |
| `_from_baseline` | `x(t) - x(t0)` | depuis le premier cycle |
| `_mean_{5,10,20}` | moyenne glissante | 5, 10, 20 cycles |
| `_std_{5,10,20}` | écart-type glissant | 5, 10, 20 cycles |

```python
grouped = data.groupby("unit_number", sort=False)[sensor]

data[f"{sensor}_delta"] = grouped.diff().fillna(0)
data[f"{sensor}_from_baseline"] = data[sensor] - grouped.transform("first")

for window in (5, 10, 20):
    data[f"{sensor}_mean_{window}"] = grouped.transform(
        lambda s: s.rolling(window, min_periods=1).mean()
    )
    data[f"{sensor}_std_{window}"] = grouped.transform(
        lambda s: s.rolling(window, min_periods=2).std()
    ).fillna(0)
```

### Causalité

Toutes les fenêtres glissantes sont **strictement causales** : une statistique au
cycle `t` ne dépend que des cycles `≤ t`. C'est la condition pour que le modèle
soit déployable en flux. Une version non causale donnerait de meilleurs scores
en validation, et serait inutilisable en production.

### Traitement des valeurs manquantes

L'écart-type d'une fenêtre de 2 cycles est indéfini au premier cycle
(`min_periods=2`), et les valeurs inutilisables sont remplies à `0` avant
inférence :

```python
X = df[feature_columns].replace([np.inf, -np.inf], np.nan).fillna(0)
```

### Performance

Les 136 colonnes sont assemblées par un `pd.concat` unique plutôt qu'insérées
une à une. En flux, l'historique est recalculé à chaque message : l'insertion
colonne par colonne sur un DataFrame de plus de 100 colonnes provoque une
fragmentation de la mémoire et un `PerformanceWarning` qui dégradait fortement
le débit de messages traités.

---

## 5. Modélisation

### Modèle retenu

**Random Forest Regressor**, choisi pour trois raisons :

- **Pas de mise à l'échelle nécessaire.** Les arbres décident sur des
  comparaisons de seuils ; standardiser les variables n'apporte rien. Si un
  modèle sensible à l'échelle est introduit plus tard, son scaler doit être
  ajusté sur les seuls moteurs d'entraînement, jamais sur l'ensemble.
- **Interprétabilité.** L'importance des variables reste consultable, ce qui
  compte quand la sortie alimente une décision d'intervention.
- **Coût d'entraînement faible** sur 20 000 lignes et 155 variables.

Le notebook compare également `ExtraTreesRegressor`, `GradientBoostingRegressor`
et `HistGradientBoostingRegressor` avant de trancher.

### Validation : `GroupShuffleSplit`

```python
GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
```

Le découpage se fait **par moteur entier**, jamais par ligne. Un découpage
aléatoire par ligne placerait des cycles d'un même moteur en entraînement et en
validation : le modèle reconnaîtrait la signature d'un moteur plutôt que son
profil de dégradation, et le score de validation serait trop optimiste. `unit_number` est
donc une clé de groupement, jamais une feature — le notebook l'affirme
explicitement :

```python
assert "unit_number" not in FEATURE_COLUMNS
```

---

## 6. Évaluation

### Score NASA asymétrique

Le score officiel pénalise différemment selon le sens de l'erreur
`d = RUL prédite − RUL réelle` :

```python
def nasa_asymmetric_score(y_true, y_pred):
    error = np.asarray(y_pred) - np.asarray(y_true)
    penalties = np.where(
        error < 0,
        np.exp(-error / 13) - 1,   # sous-estimation
        np.exp(error / 10) - 1,    # surestimation
    )
    return float(penalties.sum())
```

La pénalité est **exponentielle** et croît plus vite pour une
**surestimation** (diviseur 10) que pour une sous-estimation (diviseur 13) :
une erreur de +30 cycles coûte 19,1, une erreur de −30 cycles coûte 9,0.
Surestimer revient à maintenir en service un moteur qui va défaillir ;
sous-estimer conduit à une intervention prématurée, moins grave.

Deux conséquences pour la lecture du score :

- c'est une **somme**, pas une moyenne : il n'est comparable qu'entre jeux de
  même taille (100 moteurs pour le test officiel) ;
- il est dominé par les quelques plus grosses erreurs. Une seule surestimation
  de 100 cycles pèse 22 000 points. Il se lit donc toujours avec le MAE et le
  RMSE.

Plus le score est faible, meilleur est le modèle.

### Analyses complémentaires

Le notebook va au-delà du score officiel, car un score agrégé masque les
pannes ratées :

- **Erreurs signées** : surestimations contre sous-estimations, moyenne de
  chaque camp. Un modèle qui surestime systématiquement est inutilisable.
- **Erreurs par tranche de RUL réelle** : la difficulté de prédiction n'est pas
  uniforme, les faibles RUL étant les plus difficiles et les plus importantes.
- **Matrice de confusion métier** : les quatre décisions de maintenance
  confrontées, pour mesurer combien de situations *Critique* réelles ont été
  classées *Inspection planifiée*. Ce sont les faux négatifs coûteux.
- **Pannes critiques manquées** : la liste explicite des cas
  `RUL_réelle ≤ 20` et `RUL_prédite > 20`.

---

## 7. Modèles sérialisés

Chaque `.joblib` est un dictionnaire :

```python
{
    "model": RandomForestRegressor(...),
    "feature_columns": ["sensor_2", ..., "setting_1", "setting_2", "sensor_2_delta", ...],
    "rul_cap": 125,   # modèle de RUL uniquement
    ...
}
```

`feature_columns` est indispensable. Un Random Forest est un ensemble d'arbres
indexant les colonnes **par position** : réordonner ou renommer les variables en
inférence produit une prédiction plausible et fausse, sans lever d'erreur. Le
consumer sélectionne donc explicitement :

```python
X_latest = latest[feature_columns]
```

et refuse de continuer si une colonne attendue est absente.

---

## 8. Pistes d'optimisation

Par ordre de rendement décroissant, pour un passage en production :

1. **Features incrémentales.** Le consumer recalcule aujourd'hui 136 features
   sur l'intégralité de l'historique du moteur, à chaque message. Un état par
   moteur — dernières valeurs, sommes et sommes de carrés des fenêtres — permet
   de ne calculer que les nouvelles colonnes. Le gain est linéaire en
   longueur d'historique.

2. **Historique persistant.** L'historique vit en mémoire : au redémarrage,
   chaque moteur doit réaccumuler 20 cycles. Redis, InfluxDB ou TimescaleDB
   lèvent cette contrainte et permettent de traiter plusieurs instances de
   consumer en parallèle.

3. **Modèle allégé.** 233 Mo pour le modèle de RUL est lourd à charger et à
   répliquer. `HistGradientBoostingRegressor`, ou une réduction du nombre
   d'arbres, ferait tomber ce coût — avec un impact à mesurer sur le score.

4. **Feature importance.** Certaines des 155 variables sont probablement
   redondantes. Une sélection par importance ou par élimination récursive
   réduirait la latence et la taille du modèle sans perte de précision.

5. **Suivi de dérive.** Le jeu C-MAPSS est une simulation figée, publiée en 2008. En
   production, les capteurs dérivent : un suivi de la distribution des features
   et une alerte sur son écart à la distribution d'entraînement sont
   nécessaires.
