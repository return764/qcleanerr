FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

ENV CHECK_INTERVAL_SECONDS=600
ENV STALL_TIMEOUT_SECONDS=3600

#Setup these variables at runtime:
#ENV RADARR_URL='http://127.0.0.1:7878'
#ENV RADARR_API_KEY=

#ENV SONARR_URL='http://127.0.0.1:8989'
#ENV SONARR_API_KEY=

WORKDIR /app

COPY main_ai.py /app/main.py

RUN pip install --no-cache-dir requests

CMD ["python", "main.py"]
