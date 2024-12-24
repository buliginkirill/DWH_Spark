FROM openjdk:21-jdk-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client \
    libpq-dev \
    libffi-dev \
    build-essential \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    zlib1g-dev \
    && wget https://www.python.org/ftp/python/3.12.0/Python-3.12.0.tgz && \
    tar -xzf Python-3.12.0.tgz && \
    cd Python-3.12.0 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && \
    rm -rf Python-3.12.0 Python-3.12.0.tgz && \
    apt-get remove -y wget gnupg && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


WORKDIR /app
COPY ./input /app/input
COPY ./jdbc /app/jdbc
COPY ./src /app/src
COPY ./main.py /app/main.py
COPY ./requirements.txt /app/requirements.txt

RUN pip3.12 install --upgrade pip
RUN pip3.12 install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python3.12", "main.py"]
