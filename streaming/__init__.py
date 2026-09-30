"""Pipeline Kafka de maintenance predictive.

Dossier nomme ``streaming`` et non ``kafka`` : un package local du meme nom
que la librairie cliente ``kafka-python`` masquerait ses imports, rendant
``python -m streaming.consumer`` inutilisable.

Trois executables :

- ``producer``    : diffuse les mesures capteurs C-MAPSS sur ``sensor-data``
- ``consumer``    : predit la RUL en flux et publie sur ``rul-predictions``
- ``load_models`` : compare les bundles de modeles serialises
"""
