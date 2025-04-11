## Image
FROM ubuntu:22.04

## Disabling installation of optional dependencies for all invocations of apt-get (reduces image size)
RUN echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/00-docker
RUN echo 'APT::Install-Recommends "0";' >> /etc/apt/apt.conf.d/00-docker

## Update package list before installing any package
RUN apt update

# Change working dir
WORKDIR /app

# Exposing port
EXPOSE 8000

# Keep container running to allow volumes access
CMD ["tail", "-f", "/dev/null"]
