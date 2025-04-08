## Image
FROM ubuntu:25.04

## Disabling installation of optional dependencies for all invocations of apt-get (reduces Ubuntu image size)
RUN echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/00-docker
RUN echo 'APT::Install-Recommends "0";' >> /etc/apt/apt.conf.d/00-docker

## Update package list before installing any package
RUN apt update

## Install Python 3.10.0 + pip + OpenJDK17
RUN apt install -y python3.10 python3.10-dev && \
    apt install -y python3-pip  && \
    apt install -y openjdk-17-jdk

# Change working dir
WORKDIR /app

# Requirements copy & install
COPY src/common/common_requirements.txt .
COPY src/extract/extract_requirements.txt .

RUN python3.10 -m pip install --no-cache-dir -r common_requirements.txt && \
    python3.10 -m pip install --no-cache-dir -r extract_requirements.txt

# Copying source code
COPY src/extract /app/extract
COPY src/common /app/common

# Adjusting Python Path to be able to import common as well
ENV PYTHONPATH=/app/src

# Exposing port
EXPOSE 8001

# Execute the script
CMD ["python3.10", "src/extract/extract.py"]
