# Stage 1: Build stage - Using Ubuntu LTS as the base
FROM ubuntu:22.04 AS builder

# Set the working directory in the container
WORKDIR /app

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install core dependencies including gnupg, Python 3.11, pip, venv, and build tools
# 'gnupg' is required to securely add the PPA
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gnupg \
    software-properties-common \
    build-essential && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-venv \
    python3-pip \
    python3.11-dev && \
    rm -rf /var/lib/apt/lists/*

# Update alternatives to make python3.11 the default python3
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Copy the requirements file first to leverage Docker's layer caching
COPY requirements.txt .

# Create a virtual environment and install the dependencies
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir -r requirements.txt

# ---

# Stage 2: Final stage - the actual image we will run
FROM ubuntu:22.04

# Set the working directory
WORKDIR /app

# Install Python 3.11 runtime from PPA, including gnupg
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gnupg \
    software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-venv && \
    rm -rf /var/lib/apt/lists/*
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/venv /opt/venv

# Copy the application code
COPY . .

# Set the PATH environment variable to include the venv's bin directory
ENV PATH="/opt/venv/bin:$PATH"

# Command to run the application when the container starts
CMD ["python3", "main.py"]
