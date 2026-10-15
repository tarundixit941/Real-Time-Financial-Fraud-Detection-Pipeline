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


## ⚡ High-Throughput Cluster Optimization (Scaling Step)

To maximize data pipeline parallel ingestion speeds and avoid bottleneck lags under heavy processing bursts, scale the active Kafka topic partitions from a single instance up to a distributed multi-threaded model.

Execute this partition modification shell directly inside the running Kafka broker terminal layer:

```bash
docker exec -it config-kafka-1 kafka-topics --alter --topic financial-transactions --partitions 3 --bootstrap-server localhost:9092
```

*Note: Scaling partitions to `3` enables Apache Spark to map multiple concurrent executor threads directly to separate data queues, multiplying real-time stream ingestion throughput.*
