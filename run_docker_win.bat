@echo off

REM Set variables for container and image names
set "IMAGE_NAME=bitnami/spark:latest"
set "CONTAINER_NAME=my_pyspark_sensor_container"

REM Check if the base image exists; if not, pull it
for /f "tokens=*" %%i in ('docker images -q %IMAGE_NAME%') do set "BASE_IMAGE_EXISTS=%%i"
if defined BASE_IMAGE_EXISTS (
    echo Base image %IMAGE_NAME% already exists. Using it as the base image.
) else (
    echo Base image %IMAGE_NAME% does not exist. Pulling image...
    docker pull %IMAGE_NAME%
)

REM Step 1: Check if the Docker container exists and remove it if it does
for /f "tokens=*" %%i in ('docker ps -aq -f "name=%CONTAINER_NAME%"') do set "CONTAINER_EXISTS=%%i"
if defined CONTAINER_EXISTS (
    echo Container %CONTAINER_NAME% already exists. Removing it...
    docker rm -f %CONTAINER_NAME%
)

REM Step 2: Build the Docker image
echo Building Docker image...
docker build -t %IMAGE_NAME% .

REM Step 3: Run the Docker container
echo Running Docker container...
docker run -it --name %CONTAINER_NAME% -v "%cd%/results:/app/results" %IMAGE_NAME%
docker rm %CONTAINER_NAME%