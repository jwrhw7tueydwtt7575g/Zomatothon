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
