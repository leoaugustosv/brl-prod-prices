## Image
FROM ubuntu:22.04

## Disabling installation of optional dependencies for all invocations of apt-get (reduces Ubuntu image size)
RUN echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/00-docker
RUN echo 'APT::Install-Recommends "0";' >> /etc/apt/apt.conf.d/00-docker

## Update package list before installing any package
RUN apt update

## Install Python 3.10.0 + pip + OpenJDK17
RUN apt-get install -y python3.10 && \
    apt-get install -y python3.10-dev && \
    apt-get install -y python3-pip && \
    apt-get install -y openjdk-17-jdk


# Change working dir
WORKDIR /app

# Remove Derby lock files before exec
# RUN find . -name "*.lck" -type f -delete

# Requirements copy & install
COPY src/common/common_requirements.txt .

RUN python3.10 -m pip install --no-cache-dir -r common_requirements.txt

# Copying source code
COPY src/transform /app/transform
COPY src/common /app/transform/common

# Adjusting Python Path to be able to import common as well
ENV PYTHONPATH=/app/

# Exposing port
EXPOSE 8002

# Execute the script
CMD ["python3.10", "./transform/transform.py"]
