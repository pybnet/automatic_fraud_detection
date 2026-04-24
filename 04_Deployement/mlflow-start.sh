#!/bin/bash
pip install psycopg2-binary boto3 --quiet
mlflow server \
  --host 0.0.0.0 \
  --port 5000 \
  --backend-store-uri postgresql://fraud_user:fraud_pass@postgres:5432/mlflow_db \
  --default-artifact-root s3://mlflow-artifacts \
  --workers 1
 