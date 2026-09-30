"""Utilitaire de diagnostic : compare les bundles de modeles serialises.

Le notebook d'exploration exporte deux modeles par sous-ensemble C-MAPSS :

- ``random_forest_<ID>.joblib``      : regression de la RUL
- ``random_forest_alert_<ID>.joblib`` : classification d'alerte imminente

Ce script verifie que les deux fichiers existent, affiche le contenu de chaque
bundle et signale toute divergence entre les listes de features. Une divergence
signifie que le modele d'alerte a ete entraine sur un jeu de features
different du modele de RUL : l'inference deFlux doit alors etre realignee.

Execution
---------
    python -m streaming.load_models
"""

from __future__ import annotations

import os
import sys

import joblib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import config  # noqa: E402


def describe(label: str, path: str) -> dict:
    """Charge un bundle et affiche son type et ses cles."""
    print(f"\n=== Modele {label} ===")
    print(f"Chemin : {path}")

    if not os.path.exists(path):
        print("Fichier introuvable. Reentrainez via notebooks/preprocess.ipynb")
        return {}

    bundle = joblib.load(path)

    print(f"Type : {type(bundle)}")

    if isinstance(bundle, dict):
        print(f"Cles : {list(bundle.keys())}")
        return bundle

    return {}


def main() -> None:
    """Compare les features des deux bundles et signale les ecarts."""
    print(f"Sous-ensemble C-MAPSS : {config.DATASET_ID}")
    print(f"Dossier des modeles   : {config.MODEL_DIR}")

    rul_bundle = describe("RUL", config.RUL_MODEL_PATH)
    alert_bundle = describe("Alert", config.ALERT_MODEL_PATH)

    if not rul_bundle or not alert_bundle:
        print("\nComparaison impossible : au moins un bundle est manquant.")
        return

    rul_features = rul_bundle["feature_columns"]
    alert_features = alert_bundle["feature_columns"]

    print(f"\nNombre de features RUL   : {len(rul_features)}")
    print(f"Nombre de features Alert : {len(alert_features)}")

    identical = rul_features == alert_features
    print(f"\nFeatures identiques : {identical}")

    if not identical:
        print("\nDifferences detectees :")
        print("Presentes dans RUL mais absentes d'Alert :")
        print(set(rul_features) - set(alert_features))
        print("Presentes dans Alert mais absentes de RUL :")
        print(set(alert_features) - set(rul_features))


if __name__ == "__main__":
    main()
