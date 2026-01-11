FROM python:3.11-slim-bookworm

WORKDIR /app
RUN pip install --no-cache-dir asyncua

COPY server.py /app/server.py

EXPOSE 4840
CMD ["python", "/app/server.py"]
