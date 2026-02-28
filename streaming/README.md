# Zomato Streaming Pipeline

End-to-end Kafka streaming pipeline for Zomato order data with ML model training.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  kafka_producer │────▶│   Kafka Topic   │────▶│  kafka_consumer │
│  (10 orders/s)  │     │  zomato-orders  │     │    (S3 Sink)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │  S3 Bucket      │
                                                │  (Parquet)      │
                                                └─────────────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │  train_model.py │
                                                │  (XGBoost+MLflow│
                                                └─────────────────┘
```

## Components

### 1. Kafka Producer (`kafka_producer.py`)
- Generates synthetic Zomato orders (25 fields)
- Configurable rate: ~10 orders/second (t3.micro friendly)
- Streams JSON to `zomato-orders` topic

### 2. Kafka Consumer (`kafka_consumer.py`)
- Consumes from `zomato-orders` topic
- Batches: 100 records OR 60 seconds (whichever first)
- Writes Parquet to `s3://zomatoovertomato/raw-orders/`
- Local CSV backup in `./data/backup/`

### 3. Model Training (`train_model.py`)
- Loads data from S3 (or local backup)
- Signal Audit: GPS-based dirty label detection
- Label Decontamination: exclude/weighted/confidence strategies
- Trains XGBoost with MLflow tracking
- Saves model to `s3://zomatoovertomato/models/`

### 4. Orchestration (`run_pipeline.sh`)
- Start/stop all components
- Status monitoring
- Log management

## Quick Start

### Prerequisites
```bash
# Install Python dependencies
pip install -r requirements.txt

# Set up AWS credentials
aws configure

# Ensure Kafka is installed (set KAFKA_HOME)
export KAFKA_HOME=~/kafka
```

### Run Locally
```bash
# Start full pipeline
./run_pipeline.sh start

# Check status
./run_pipeline.sh status

# View logs
./run_pipeline.sh logs

# Run model training
./run_pipeline.sh train

# Stop everything
./run_pipeline.sh stop
```

### Run Individual Components
```bash
# Producer only
python3 kafka_producer.py --rate 10 --topic zomato-orders

# Consumer only
python3 kafka_consumer.py --s3-bucket zomatoovertomato --batch-size 100

# Training only
python3 train_model.py --strategy weighted --mlflow-uri http://localhost:5000
```

## EC2 Deployment

### Copy files to EC2
```bash
scp -i your-key.pem -r streaming/ ec2-user@<EC2-IP>:~/zomato-pipeline/
```

### Install systemd services
```bash
cd ~/zomato-pipeline/streaming/systemd
chmod +x install_services.sh
./install_services.sh
```

### Start services
```bash
sudo systemctl start kafka
sleep 10
sudo systemctl start zomato-mlflow
sudo systemctl start zomato-producer
sudo systemctl start zomato-consumer
```

## Configuration

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka brokers |
| `KAFKA_TOPIC` | zomato-orders | Topic name |
| `S3_BUCKET` | zomatoovertomato | S3 bucket |
| `S3_PREFIX` | raw-orders | S3 key prefix |
| `AWS_REGION` | eu-north-1 | AWS region |
| `ORDERS_PER_SECOND` | 10 | Producer rate |
| `BATCH_SIZE` | 100 | Consumer batch size |
| `BATCH_TIMEOUT_SECONDS` | 60 | Batch timeout |
| `MLFLOW_TRACKING_URI` | http://localhost:5000 | MLflow server |

## Order Schema (25 fields)

| Field | Type | Description |
|-------|------|-------------|
| order_id | string | Unique order identifier |
| merchant_id | string | Restaurant identifier |
| restaurant_name | string | Restaurant name |
| city | string | City (Mumbai, Delhi, etc.) |
| cuisine_type | string | Cuisine category |
| restaurant_lat | float | Restaurant latitude |
| restaurant_lng | float | Restaurant longitude |
| order_confirmed_at | datetime | Order confirmation time |
| for_timestamp | datetime | Food Ready timestamp |
| rider_arrived_at | datetime | Rider arrival time |
| rider_id | string | Rider identifier |
| rider_lat | float | Rider GPS latitude |
| rider_lng | float | Rider GPS longitude |
| num_items | int | Number of items |
| item_category | string | Item category |
| item_complexity_score | float | Complexity (0.2-1.8) |
| concurrent_orders | int | Concurrent kitchen orders |
| kitchen_load_index | float | Kitchen load (0.1-1.0) |
| is_peak_hour | bool | Peak hour flag |
| day_of_week | string | Day name |
| is_weekend | bool | Weekend flag |
| weather_condition | string | Weather |
| actual_kpt_seconds | int | Kitchen Prep Time (label) |
| is_for_rider_influenced | bool | Rider-influenced flag |
| label_confidence | float | Label confidence (0.4-1.0) |

## MLflow Dashboard

Access at: `http://<EC2-IP>:5000`

Tracks:
- Model parameters (XGBoost hyperparameters)
- Metrics (MAE, RMSE, P50, P90, P95)
- Artifacts (model, feature importance)
- Model registry

## License

MIT
