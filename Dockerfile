FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY db ./db
COPY gx ./gx
COPY core ./core
COPY scripts ./scripts
COPY test_data ./test_data
COPY templates ./templates
COPY repository ./repository
COPY file_parser ./file_parser
COPY data_loader ./data_loader
COPY template_engine ./template_engine
COPY validation_engine ./validation_engine

ENTRYPOINT ["python", "main.py"]