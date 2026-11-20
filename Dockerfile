FROM apache/airflow:2.9.2

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    fastparquet \
    beautifulsoup4 \
    requests \
    pyyaml \
    numpy \
    rasterio
