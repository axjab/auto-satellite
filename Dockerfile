FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml ./
COPY auto_satellite.py ./

RUN pip install --no-cache-dir .

EXPOSE 8090

CMD ["auto-satellite"]
