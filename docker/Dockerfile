# One image for API, Streamlit, ETL, training, Kafka consumer and Spark streaming (different commands).
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir mlflow==2.17.2
COPY backend/src ./backend/src
COPY backend/api ./backend/api
COPY streamlit_app ./streamlit_app
COPY db ./db
COPY dbt_project ./dbt_project
COPY scripts ./scripts
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /app/data/raw /app/data/processed /app/data/checkpoints /app/data/stream_in /app/models /app/state \
    && chown -R app:app /app
USER app
