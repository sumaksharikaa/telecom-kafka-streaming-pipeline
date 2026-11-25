# Pipeline image: Python + Java 17 for Spark, with the package installed.
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    TELECOM_DATA_ROOT=/data

WORKDIR /app
COPY pyproject.toml README.md ./
COPY conf ./conf
COPY src ./src
COPY scripts ./scripts
COPY tests ./tests
RUN pip install --no-cache-dir -e ".[dev]"

CMD ["python", "-m", "telecom_stream.streaming_job", "--trigger", "continuous"]
