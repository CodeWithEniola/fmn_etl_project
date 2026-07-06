# Use Apache Airflow 3.2.2 with Python 3.12
FROM apache/airflow:3.2.2-python3.12

# Switch to root to install system packages
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        git \
        curl \
        && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements.txt to the container
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

COPY dags/ /opt/airflow/dags/

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow/dags
