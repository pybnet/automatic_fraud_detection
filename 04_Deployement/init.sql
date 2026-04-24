-- Création de la base MLflow (doit être hors transaction)
SELECT 'CREATE DATABASE mlflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow_db')\gexec

-- Schéma des prédictions (dans fraud_db)
CREATE TABLE IF NOT EXISTS predictions (
    id              SERIAL PRIMARY KEY,
    trans_num       VARCHAR(64) UNIQUE NOT NULL,
    cc_num          BIGINT,
    merchant        VARCHAR(255),
    category        VARCHAR(100),
    amt             NUMERIC(10, 2),
    city            VARCHAR(100),
    state           VARCHAR(10),
    is_fraud_true   INTEGER,
    is_fraud_pred   INTEGER,
    fraud_score     NUMERIC(6, 4),
    predicted_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_predictions_trans_num     ON predictions(trans_num);
CREATE INDEX IF NOT EXISTS idx_predictions_predicted_at  ON predictions(predicted_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_is_fraud_pred ON predictions(is_fraud_pred);
