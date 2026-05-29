FROM apache/airflow:2.9.1

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME es obligatorio para PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt