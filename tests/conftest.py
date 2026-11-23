import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
for carpeta in ("jobs", "despliegues", "tests"):
    sys.path.insert(0, str(RAIZ / carpeta))
