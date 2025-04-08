## Image
FROM ubuntu:25.04

## Disabling installation of optional dependencies for all invocations of apt-get (reduces image size)
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
RUN python3.10 -m pip install --no-cache-dir -r common_requirements.txt

# Copying source code
COPY src/common /app/

# Exposing port
EXPOSE 8000

# Keep container running to allow volumes access
CMD ["tail", "-f", "/dev/null"]
