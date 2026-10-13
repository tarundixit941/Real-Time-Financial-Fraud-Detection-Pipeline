# Real-Time-Financial-Fraud-Detection-Pipeline
A streaming data analytics pipeline designed to monitor financial transactions in real-time. This system evaluates incoming transactions against historical baselines and user behavioral profiles to instantly flag anomalous, potentially fraudulent activities before funds are settled.

# Real-Time Financial Fraud Detection Pipeline

## 📊 Analytics Infrastructure & Port Access Rules

This document outlines the exposed network topology, system endpoints, and infrastructure management credentials for the data pipeline monitoring services.

### 🌐 System Network Mappings

| Service Name | Exposed Port | Internal Cluster Port | Description |
| :--- | :--- | :--- | :--- |
| **Apache Kafka** | `9092` | `29092` | Stream message ingestion pipeline loop network |
| **Apache Cassandra** | `9042` | `9042` | High-throughput analytical storage sink table |
| **Prometheus** | `9090` | `9090` | Time-series system metrics collection engine |
| **Grafana Dashboard** | `3000` | `3000` | Executive real-time visual charts UI |

### 🔐 System Authentication Baseline

*   **Grafana Administrative Web UI:**
    *   **Endpoint Target Link:** `http://localhost:3000`
    *   **Default Username:** `admin`
    *   **Default Password:** `admin`
    *   *Note: Security policy mandates password rotation instantly upon initialization.*
*   **Apache Cassandra Database Access:**
    *   **Native Endpoint connection:** `localhost:9042`
    *   **Default Command Line Interface CLI Shell Tool:** `cqlsh`