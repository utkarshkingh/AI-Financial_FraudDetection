FROM apache/airflow:2.10.5

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      gnupg \
      gcc \
      g++ \
      build-essential \
      python3-dev \
      libssl-dev \
      libsasl2-dev \
      pkg-config \
      ca-certificates \
      && apt-get clean && \
      rm -rf /var/lib/apt/lists/*

# Configure SSL certificates
RUN ln -sf /etc/ssl/certs/ca-certificates.crt /usr/lib/ssl/cert.pem

# Create and set permissions for all required application directories
RUN mkdir -p /app/logs /app/models /app/cache && chown -R airflow: /app/logs /app/models /app/cache

# Copy and install Python requirements
COPY requirements.txt /tmp/requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt
USER root
RUN rm -f /tmp/requirements.txt
USER airflow

# Set the working directory
WORKDIR /app

# Set environment for SSL compatibility
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
