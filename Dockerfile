FROM apache/airflow:2.9.2

# Workshop #3: PySpark needs a Java runtime inside the Airflow image
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Versions pinned with the official Airflow 2.9.2 constraints file so the build is
# reproducible and pip cannot upgrade packages Airflow itself depends on.
# rasterio and pyspark are not in the constraints file, so they are pinned explicitly.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt" \
    pandas \
    pyarrow \
    beautifulsoup4 \
    requests \
    pyyaml \
    numpy \
    rasterio==1.4.3 \
    pyspark==3.5.3
