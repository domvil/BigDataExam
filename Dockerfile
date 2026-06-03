FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/workspace/src \
    SPARK_LOCAL_IP=127.0.0.1 \
    MPLBACKEND=Agg

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY README.md ./README.md
COPY REPORT.md ./REPORT.md

CMD ["python", "-m", "ais_collision.batch", "--input-glob", "/workspace/Data/aisdk-2021-12-*.csv", "--output-dir", "/workspace/output/full-month", "--driver-memory", "8g", "--master", "local[4]", "--shuffle-partitions", "200"]