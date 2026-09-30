"""Telechargement du jeu de donnees C-MAPSS de la NASA.

Recupere l'archive officielle et l'extrait dans ``dataset/raw``. Le script est
idempotent : si l'archive est deja presente, il ne retelecharge rien.

Execution
---------
    python -m src.download
"""

from __future__ import annotations

import os
import sys
import urllib.request
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import config  # noqa: E402

#: Miroirs directs de l'archive C-MAPSS, essaies dans l'ordre.
CMAPSS_ARCHIVE_URLS = [
    "https://github.com/philippe-e/C-MAPSS-Data/raw/master/CMAPSSData.zip",
    "https://raw.githubusercontent.com/datasets/cmapss/main/CMAPSSData.zip",
]

#: En-tete de requete : plusieurs miroirs renvoient 403 sans User-Agent.
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def _download_archive(url: str, zip_path: str) -> bool:
    """Telecharge une URL vers ``zip_path``. Retourne True en cas de succes."""
    request = urllib.request.Request(url, headers=REQUEST_HEADERS)

    try:
        with urllib.request.urlopen(request) as response:
            with open(zip_path, "wb") as archive:
                archive.write(response.read())
        return True
    except Exception as error:  # noqa: BLE001 - on rapporte puis on essaie le suivant
        print(f"Echec du telechargement depuis {url} : {error}")
        return False


def download_and_extract_cmapss(raw_data_dir: str | None = None) -> str | None:
    """Telecharge et extrait C-MAPSS. Retourne le dossier d'extraction ou None."""
    target_dir = raw_data_dir or config.RAW_DATA_DIR
    os.makedirs(target_dir, exist_ok=True)

    zip_path = os.path.join(target_dir, "CMAPSSData.zip")

    if not os.path.exists(zip_path):
        for url in CMAPSS_ARCHIVE_URLS:
            print(f"Telechargement de C-MAPSS depuis {url}...")
            if _download_archive(url, zip_path):
                print("Telechargement termine.")
                break
        else:
            print("Aucun miroir n'a repondu. Verifiez la connexion reseau.")
            return None

    print("Extraction des fichiers...")

    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            archive.extractall(target_dir)
    except zipfile.BadZipFile:
        print("Archive corrompue, suppression. Relancez le script.")
        os.remove(zip_path)
        return None

    print(f"Fichiers extraits dans '{target_dir}'.")
    return target_dir


if __name__ == "__main__":
    download_and_extract_cmapss()
