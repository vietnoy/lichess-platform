FROM python:3.13-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN pip install -r requirements.txt

# Mirror the import path expected by worker.py (`from services.analyzer.stockfish import eval_fen`).
COPY worker.py stockfish.py bootstrap.py /app/services/analyzer/
RUN touch /app/services/__init__.py /app/services/analyzer/__init__.py

CMD ["python", "-m", "services.analyzer.worker"]
