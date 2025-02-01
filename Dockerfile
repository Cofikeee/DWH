# Use Apache Airflow's official Docker image as a base
FROM apache/airflow:2.10.3

# Set the working directory
WORKDIR /opt/airflow

# Switch to root user to install dependencies
USER root

# Install build dependencies
RUN apt-get update && \
    apt-get install -y gcc libffi-dev python3-dev && \
    apt-get clean

# Copy the requirements file into the image
COPY requirements.txt .

# Switch back to the airflow user for pip installation
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
