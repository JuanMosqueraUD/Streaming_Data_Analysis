FROM apache/airflow:2.9.1

# Cambia a root para instalar dependencias del sistema si las necesitas
USER root

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Vuelve al usuario airflow para instalar paquetes Python
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt