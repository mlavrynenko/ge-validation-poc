FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY db ./db
COPY gx ./gx
COPY core ./core
COPY test_data ./test_data
COPY repository ./repository
COPY data_loader ./data_loader
COPY validation_engine ./validation_engine

ENTRYPOINT ["python", "main.py"]