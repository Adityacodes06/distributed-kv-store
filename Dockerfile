# ── Stage: runtime ──────────────────────────────────────────
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY client.py .

# Create data directory for WAL and SQLite
RUN mkdir -p /app/data

# Expose the default port
EXPOSE 8000

# Health check — Docker will restart unhealthy containers
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD python -c "import httpx; httpx.get('http://localhost:8000/health').raise_for_status()"

# Run the FastAPI server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
