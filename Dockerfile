# ==============================================================================
# GeoMesh-Stream: Real-Time Telemetry & Spatial Geofence Stream Engine
# ==============================================================================
FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir fastapi uvicorn

COPY geomesh ./geomesh
RUN pip install --no-cache-dir -e .

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["geomesh", "serve", "--host", "0.0.0.0", "--port", "8080"]
