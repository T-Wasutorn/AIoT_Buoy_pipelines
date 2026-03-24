FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    psycopg2-binary \
    pika \
    pandas \
    joblib \
    scikit-learn \
    xgboost

COPY . .

CMD ["python", "consumer_ml.py"]