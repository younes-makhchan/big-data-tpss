# Kafka Fraud Detection

Kafka Fraud Detection is a real-time system designed to detect and analyze potentially fraudulent transactions. It leverages **Kafka Streams** for data processing, **InfluxDB** for storage, and **Grafana** for visualizing results on dynamic dashboards.

---

## Architecture Overview

The system architecture is illustrated below:

![System Architecture](images/system.png)

### Core Components:
- **Kafka Streams**: Handles real-time processing of transaction data streams.
- **InfluxDB**: Serves as the data repository for transactions and flagged anomalies.
- **Grafana**: Displays insights and flagged activities through interactive dashboards.

---

## Example Suspicious Transaction Logs

The system logs suspicious activity in the following format:

![Example Logs](images/logs.png)

---

## Dashboards

Visualize transaction data and anomalies using Grafana:

### Detailed Transaction Analytics
![Transaction Insights](images/grafana2.png)

---

## InfluxDB

InfluxDB is used to store and query transaction data:
![InfluxDB Query Example](images/influxDB.png)

---

## Getting Started

To set up and use the Kafka Fraud Detection system, follow these steps:

1. **Start Services**: Launch the Kafka and InfluxDB services.
2. **Transaction Input**: Use the Kafka Producer to send transaction data to the `transactions-input` topic.
3. **Monitor Dashboards**: Access Grafana dashboards to view transaction trends and flagged anomalies.

---

## Features

- **Real-Time Detection**: Detect and flag suspicious transactions as they occur.
- **Powerful Querying**: Leverage InfluxDB for flexible and efficient querying.
- **Visual Analytics**: Explore data trends and anomalies with Grafana dashboards.

---


