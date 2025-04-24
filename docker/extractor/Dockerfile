## Image
FROM ubuntu:22.04

## Disabling installation of optional dependencies for all invocations of apt-get (reduces Ubuntu image size)
RUN echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/00-docker
RUN echo 'APT::Install-Recommends "0";' >> /etc/apt/apt.conf.d/00-docker

## Update package list before installing any package
RUN apt update 

## Install utilities
RUN apt install wget unzip curl -y

## Install Python 3.10.0 + pip + OpenJDK17
RUN apt-get install -y python3.10 && \
    apt-get install -y python3.10-dev && \
    apt-get install -y python3-pip && \
    apt-get install -y openjdk-17-jdk

# Install Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb && rm -f ./google-chrome-stable_current_amd64.deb

# Install Chromedriver

RUN CHROMEDRIVER_LINK=$(curl -s 'https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json' | \
    python3 -c "import sys, json; url = json.loads(sys.stdin.read()); print(url['channels']['Stable']['downloads']['chromedriver'][0]['url'])") && \
    echo "CHROMEDRIVER_LINK obtained: $CHROMEDRIVER_LINK" && \
    curl -sSL "$CHROMEDRIVER_LINK" -o /tmp/chromedriver.zip && \
    unzip /tmp/chromedriver.zip -d /usr/bin/ && \
    mv /usr/bin/chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver && \
    rm -rf /usr/bin/chromedriver-linux64 /tmp/chromedriver.zip


## Install XVFB (Display Driver)
RUN apt-get install -y \
    xvfb \
    libnss3 \
    libgconf-2-4 \
    libxi6 \
    libxcursor1 \
    libxcomposite1 \
    libxrandr2 \
    libasound2 \
    fonts-liberation \
    libappindicator1 \
    xdg-utils \
    libxpm4 \
    libxrender1 \
    libgtk2.0-0 \
    libnss3 \
    libgconf-2-4 \
    xfonts-100dpi \
    xfonts-75dpi \
    xfonts-base \
    xfonts-scalable \
    x11-apps \
    && rm -rf /var/lib/apt/lists/*


# Change working dir
WORKDIR /app

# Avoid Derby lock files error
RUN find . -name "*.lck" -type f -delete

# Requirements copy & install
COPY src/common/common_requirements.txt .
COPY src/extract/extract_requirements.txt .

RUN python3.10 -m pip install --no-cache-dir -r common_requirements.txt && \
    python3.10 -m pip install --no-cache-dir -r extract_requirements.txt

# Copying source code
COPY src/extract /app/extract
COPY src/common /app/extract/common

# Adjusting Python Path to be able to import common as well
ENV PYTHONPATH=/app/



# Adiciona apenas o necessário para interface gráfica via VNC
RUN apt-get update && apt-get install -y \
    fluxbox \
    x11vnc \
    && rm -rf /var/lib/apt/lists/*

# Exponha a porta do VNC
EXPOSE 5900


# Turn display_init executable
RUN chmod +x /app/extract/display_init.sh
ENTRYPOINT ["/app/extract/display_init.sh"]

# Execute the script
CMD ["python3.10", "./extract/extract.py"]
