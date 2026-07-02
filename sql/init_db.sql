-- Création de la base banking pour stocker les KPIs
CREATE DATABASE banking;

-- Connexion à la base banking
\c banking;

-- Table des transactions nettoyées
CREATE TABLE IF NOT EXISTS transactions (
    trans_date      DATE,
    category        VARCHAR(100),
    amount          FLOAT,
    is_fraud        INTEGER,
    merchant        VARCHAR(200),
    state           VARCHAR(10)
);

-- Table des KPIs journaliers
CREATE TABLE IF NOT EXISTS kpi_daily (
    trans_date          DATE,
    nb_transactions     INTEGER,
    total_amount        FLOAT,
    nb_fraud            INTEGER,
    fraud_rate          FLOAT
);

-- Table des KPIs par catégorie
CREATE TABLE IF NOT EXISTS kpi_by_category (
    category        VARCHAR(100),
    nb_transactions INTEGER,
    total_amount    FLOAT,
    nb_fraud        INTEGER
);
