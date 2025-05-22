# Use the Bitnami Spark image as the base
FROM bitnami/spark:latest

# Set working directory in the container
WORKDIR /app

# Copy your local project files into the container
COPY . /app

# Install Python dependencies
RUN pip install -r requirements.txt

# Create the results directory
RUN mkdir -p /app/results 

# Create the results directory for data
RUN mkdir -p /app/results/data

# Create the results directory for plots
RUN mkdir -p /app/results/plots

# Command to run the Spark job
CMD ["spark-submit", "--master", "local", "main.py"]