# Use a lightweight Python image as the base
FROM python:3.9-slim

# Argument for the Airflow version
ARG AIRFLOW_VERSION=2.8.0
ARG PYTHON_VERSION=3.9

# Export the environment variable AIRFLOW_HOME where Airflow will be installed
ENV AIRFLOW_HOME=/opt/airflow

# Set environment variables for Airflow
ENV PATH=$AIRFLOW_HOME/bin:$PATH

# Add include directory to PYTHONPATH
ENV PYTHONPATH="${AIRFLOW_HOME}/include:${PYTHONPATH}"

# Install dependencies and tools
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    python3-dev \
    build-essential \
    gcc \
    g++ \
    libpq-dev \
    libssl-dev \
    curl \
    git \
    libffi-dev \
    default-libmysqlclient-dev \
    mariadb-client \
    libmariadb-dev \
    pkg-config \
    libsasl2-dev \
    freetds-dev \
    libkrb5-dev \
    netcat-openbsd \
    unzip \
    vim \
    && apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create airflow user and group
RUN groupadd -r airflow && useradd -r -g airflow airflow

# Install Apache Airflow and its dependencies
RUN pip install "apache-airflow[celery,postgres,hive,jdbc,mysql,ssh,redis]==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install additional providers and libraries
RUN pip install \
    minio==7.1.16 \
    clickhouse-connect==0.6.4 \
    airflow-providers-clickhouse

# Create necessary directories and set permissions
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins ${AIRFLOW_HOME}/include && \
    chown -R airflow:airflow ${AIRFLOW_HOME}

# Copy the start-airflow.sh script from the host to the container
COPY ./start-airflow.sh ${AIRFLOW_HOME}/start-airflow.sh

# Set the start-airflow.sh file to be executable
RUN chmod +x ${AIRFLOW_HOME}/start-airflow.sh

# Switch to the airflow user
USER airflow

# Expose ports for Airflow webserver and scheduler
EXPOSE 8080 8793

# Set the working directory
WORKDIR ${AIRFLOW_HOME}

# Start Airflow using the entrypoint script
CMD ["./start-airflow.sh"]
