# Zomato End-to-End Streaming MLOps Pipeline (AWS EC2)

## Overview
This project demonstrates a production-grade streaming MLOps pipeline for real-time order data, model training, and experiment tracking using open-source tools and AWS services.

**Key Technologies:**
- Python, Kafka, S3, XGBoost, MLflow, systemd
- AWS EC2 (compute), S3 (data lake, model/artifact store)

## Architecture
```
+-------------------+        +-------------------+        +-------------------+
|   Kafka Producer  | -----> |   Kafka Broker    | -----> |   Kafka Consumer  |
+-------------------+        +-------------------+        +-------------------+
                                                              |
                                                              v
                                                    +-------------------+
                                                    |      S3 Bucket    |
                                                    +-------------------+
                                                              |
                                                              v
                                                    +-------------------+
                                                    |   Model Training  |
                                                    +-------------------+
                                                              |
                                                              v
                                                    +-------------------+
                                                    |     MLflow UI     |
                                                    +-------------------+
```

## Components
- **Kafka Producer**: Streams synthetic Zomato orders to Kafka topic.
- **Kafka Consumer**: Batches and writes orders to S3 as Parquet.
- **S3**: Stores raw data and trained models.
- **Model Training**: Loads from S3, trains XGBoost, logs to MLflow.
- **MLflow**: Tracks experiments, metrics, and artifacts.

## Quickstart
1. **Clone the repo:**
   ```
   git clone https://github.com/jwrhw7tueydwtt7575g/Zomatothon.git
   cd Zomatothon
   ```
2. **Install dependencies:**
   ```
   pip install -r streaming/requirements.txt
   ```
3. **Configure AWS credentials:**
   ```
   aws configure
   # Or attach an IAM role with S3 access
   ```
4. **Start Kafka, Producer, Consumer, MLflow:**
   ```
   cd streaming
   ./run_pipeline.sh start
   # Or use systemd services in streaming/systemd/
   ```
5. **Access MLflow UI:**
   - http://<EC2-IP>:5000

## Notebooks
- `pipeline_architecture.ipynb`: Visualizes and documents the full pipeline, with code to interact with S3 and MLflow.

## Folder Structure
- `streaming/` - All pipeline scripts, configs, and systemd services
- `pipeline_architecture.ipynb` - Architecture and demo notebook

## License
MIT

---

## Hackathon Prompt Coverage

This project is a complete solution for the following hackathon challenge:

> **Create a food delivery order dataset with 100,000 rows simulating Zomato-style kitchen operations. Each row represents one order. Include:**
> - **Order & Merchant Info:** Unique order ID, merchant ID (from 500), restaurant name, city (Mumbai, Delhi, Bangalore, Hyderabad, Pune, Chennai), cuisine type, restaurant lat/lon.
> - **Timestamps:** Order confirmed, FOR (Food Order Ready) = confirmed + random prep time (8–45 min), rider arrived timestamp.
> - **Rider Info:** Rider ID, rider lat/lon at FOR. 35% of rows simulate fake FOR (rider within 200m), 65% genuine (rider 500m–3km away).
> - **Order Items:** Number of items (1–6), item category, item complexity score (0.2–1.8, category-based).
> - **Kitchen Load:** Concurrent orders (1–8), kitchen load index (0.1–1.0), peak hour, day of week, weekend, weather.
> - **Labels:** Actual KPT (seconds, normal distribution by complexity/load), is FOR rider influenced (boolean), label confidence (0.4–1.0, lower for fake FOR).

**How this repo solves it:**
- Synthetic data generator (Kafka producer) creates all required fields, distributions, and correlations.
- Real-time streaming, S3 data lake, and full MLOps pipeline for model training and audit.
- MLflow for experiment tracking and artifact management.
- All code, automation, and documentation included for reproducibility and demo.

---
