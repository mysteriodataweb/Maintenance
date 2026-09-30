"""Utilitaires d'entree du projet Maintenance.

Regroupe le telechargement du jeu de donnees C-MAPSS et la configuration
centralisee lue par les scripts de producton et de consommation Kafka.
"""

from src import config
from src.download import download_and_extract_cmapss

__all__ = ["config", "download_and_extract_cmapss"]
