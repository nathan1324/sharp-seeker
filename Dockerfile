FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY sharp_seeker/ sharp_seeker/

VOLUME /app/data

ENV DB_PATH=/app/data/sharp_seeker.db

CMD ["sharp-seeker"]
