#!/bin/bash

# Set variables for container and image names
IMAGE_NAME="bitnami/spark:latest"
CONTAINER_NAME="my_pyspark_sensor_container"

# Check if the base image exists; if not, pull it
if [ "$(docker images -q $IMAGE_NAME)" ]; then
    echo "Base image $IMAGE_NAME already exists. Using it as the base image."
else
    echo "Base image $IMAGE_NAME does not exist. Pulling image..."
    docker pull $IMAGE_NAME
fi

# Step 1: Check if the Docker container exists and remove it if it does
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Container $CONTAINER_NAME already exists. Removing it..."
    docker rm -f $CONTAINER_NAME
fi

# Step 2: Build the Docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Step 3: Run the Docker container
echo "Running Docker container..."
docker run -it --name $CONTAINER_NAME -v "$(pwd)/results:/app/results" $IMAGE_NAME
docker rm $CONTAINER_NAME