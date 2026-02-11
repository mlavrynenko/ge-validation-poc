FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY validation_engine ./validation_engine
COPY data_loader ./data_loader
COPY test_data ./test_data
COPY gx ./gx

ENTRYPOINT ["python", "main.py"]