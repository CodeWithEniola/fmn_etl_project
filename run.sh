#!/bin/bash

# Exit immediately if any command fails
set -e

IMAGE_NAME="fmn_airflow:1.0"

echo "Starting the elt process..."

# Check if the Docker image already exists locally
if [ -n "$(docker images -q $IMAGE_NAME 2>/dev/null)" ]; then
    echo "Image '$IMAGE_NAME' already exists. Skipping build step."
else
    echo "Image '$IMAGE_NAME' not found. Starting Docker build..."
    
    # Script pauses here until the build is 100% complete
    docker build -t "$IMAGE_NAME" .
    
    echo "Image build complete."
fi

sleep 5  # Optional: Wait for a few seconds to ensure the build process is fully completed

# This will ONLY run if the build finished successfully or was skipped

echo "initializing airflow init"
docker-compose up airflow-init

echo "Docker containers starting."
docker-compose up -d
