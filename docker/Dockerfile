# One image for API, Streamlit, ETL, training, Kafka consumer and Spark streaming (different commands).
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir mlflow==2.17.2
COPY src ./src
COPY api ./api
COPY streamlit_app ./streamlit_app
COPY db ./db
COPY scripts ./scripts
RUN mkdir -p /app/data/raw /app/models /app/state
