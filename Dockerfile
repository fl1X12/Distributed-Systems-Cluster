FROM python:3.9-slim

WORKDIR /app

# Install Docker client for managing node containers
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://get.docker.com -o get-docker.sh && \
    sh get-docker.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY api_server.py .
COPY node_container.py .
COPY kubernetes_sim_cli.py .

# Expose the API server port
EXPOSE 5000

# Run the API server
CMD ["python", "api_server.py"]