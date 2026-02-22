# MRCA AI Tariff — Backend & pipeline runner
# Used for: API server (uvicorn), pytest (pipeline verification), ingestion DAG
FROM python:3.11-slim

WORKDIR /app

# System deps for PyMuPDF
RUN apt-get update && apt-get install -y --no-install-recommends \
    libmupdf-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/app
ENV AUDIT_LOG_DIR=/app/storage/audit
ENV RUNNER_INSIDE_CONTAINER=1

# Default: run API (overridden by pipeline runner: docker compose run backend sh -c "pytest ...")
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
