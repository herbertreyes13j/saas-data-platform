#!/usr/bin/env bash
set -euo pipefail

# Ir a la raíz del proyecto.
cd "$(dirname "$0")/.."

# Comprobar los requisitos del entorno.
command -v python3.11 >/dev/null || {
    echo "Error: se requiere Python 3.11."
    exit 1
}

command -v java >/dev/null || {
    echo "Error: se requiere Java 17."
    exit 1
}

# Crear el entorno virtual.
python3.11 -m venv .venv

# Instalar las dependencias del proyecto.
.venv/bin/python -m pip install -r requirements.txt

echo "Entorno instalado correctamente."
echo "Activa el entorno con: source .venv/bin/activate"
echo "Configura el proyecto con: export PYTHONPATH=src"
