#!/bin/bash
set -e

DATA_ZIP_URL="https://github.com/JohanDiaz9021/Retail-Transactions/releases/download/v1.0-data/data.zip"

if [ ! -d "data/gold" ] || [ -z "$(ls -A data/gold 2>/dev/null)" ]; then
    echo "=== Descargando datos desde GitHub Releases ==="
    curl -L -o data.zip "$DATA_ZIP_URL"
    echo "=== Extrayendo ==="
    unzip -o data.zip
    rm -f data.zip
    echo "=== Datos listos ==="
fi

echo "=== Iniciando Streamlit ==="
streamlit run app/streamlit_app.py \
    --server.port=${PORT:-8501} \
    --server.address=0.0.0.0
