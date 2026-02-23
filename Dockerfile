FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY pyproject.toml .
COPY sharp_seeker/ sharp_seeker/
COPY scripts/ scripts/
RUN pip install --no-cache-dir .

RUN useradd --create-home appuser
RUN mkdir -p /app/data && chown appuser:appuser /app/data

VOLUME /app/data

ENV DB_PATH=/app/data/sharp_seeker.db

USER appuser

CMD ["sharp-seeker"]
