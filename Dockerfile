FROM python:3.9-slim

ARG CLOUD_SDK_VERSION=442.0.0
ENV CLOUD_SDK_VERSION=$CLOUD_SDK_VERSION
ENV PATH "$PATH:/opt/google-cloud-sdk/bin/"

# Install gcloud SDK and other dependencies
RUN apt-get -qqy update && apt-get install -qqy \
    apt-transport-https \
    ca-certificates \
    gnupg \
    curl \
    lsb-release \
    openssh-client \
    git \
    make && \
    export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk=${CLOUD_SDK_VERSION}-0 && \
    gcloud --version && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --quiet -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

# Install BigQuery command-line tools
RUN apt-get update && apt-get install -y \
    google-cloud-sdk-bigquery \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy application files
WORKDIR /app
COPY . /app

# Make scripts executable
RUN find /app/scripts -type f -name "*.sh" -exec chmod +x {} \;

# Set environment variables
ENV PYTHONPATH=/app:$PYTHONPATH
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/service-account.json

# Default command
CMD ["python", "-m", "app"]
