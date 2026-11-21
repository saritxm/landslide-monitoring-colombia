FROM apache/airflow:2.9.2

USER airflow

# Versions pinned with the official Airflow 2.9.2 constraints file so the build is
# reproducible and pip cannot upgrade packages Airflow itself depends on.
# rasterio is not in the constraints file, so it is pinned explicitly.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt" \
    pandas \
    pyarrow \
    beautifulsoup4 \
    requests \
    pyyaml \
    numpy \
    rasterio==1.4.3
