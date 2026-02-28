#!/bin/bash
#===============================================================================
# Zomato Streaming Pipeline Orchestration Script
# 
# Starts: Kafka Broker → Producer (background) → Consumer → Model Training
# Usage: ./run_pipeline.sh [start|stop|status|train]
#===============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
DATA_DIR="${SCRIPT_DIR}/data"
LOG_DIR="${SCRIPT_DIR}/logs"
PID_DIR="${SCRIPT_DIR}/pids"

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-zomato-orders}"

# S3 settings
S3_BUCKET="${S3_BUCKET:-zomatoovertomato}"
S3_DATA_PREFIX="${S3_DATA_PREFIX:-raw-orders}"
S3_MODEL_PREFIX="${S3_MODEL_PREFIX:-models}"
AWS_REGION="${AWS_REGION:-eu-north-1}"

# MLflow settings
MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://localhost:5000}"
MLFLOW_EXPERIMENT="${MLFLOW_EXPERIMENT:-zomato-kpt-prediction}"

# Producer settings
ORDERS_PER_SECOND="${ORDERS_PER_SECOND:-10}"

# Consumer settings
BATCH_SIZE="${BATCH_SIZE:-100}"
BATCH_TIMEOUT="${BATCH_TIMEOUT:-60}"

# Training trigger
TRAINING_TRIGGER_RECORDS="${TRAINING_TRIGGER_RECORDS:-1000}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

#===============================================================================
# Helper Functions
#===============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

create_directories() {
    mkdir -p "${DATA_DIR}" "${LOG_DIR}" "${PID_DIR}" "${DATA_DIR}/backup"
    log_info "Created directories: data, logs, pids"
}

check_kafka() {
    if ! command -v "${KAFKA_HOME}/bin/kafka-topics.sh" &> /dev/null; then
        log_error "Kafka not found at ${KAFKA_HOME}"
        log_info "Set KAFKA_HOME environment variable or install Kafka"
        return 1
    fi
    return 0
}

check_python() {
    if ! command -v python3 &> /dev/null; then
        log_error "Python3 not found"
        return 1
    fi
    return 0
}

#===============================================================================
# Kafka Management
#===============================================================================

start_kafka_kraft() {
    log_info "Starting Kafka (KRaft mode)..."
    
    if pgrep -f "kafka.Kafka" > /dev/null; then
        log_warning "Kafka is already running"
        return 0
    fi
    
    # Check if KRaft is configured
    if [[ ! -f "${KAFKA_HOME}/config/kraft/server.properties" ]]; then
        log_info "Configuring KRaft..."
        CLUSTER_ID=$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)
        "${KAFKA_HOME}/bin/kafka-storage.sh" format -t "$CLUSTER_ID" \
            -c "${KAFKA_HOME}/config/kraft/server.properties" 2>/dev/null || true
    fi
    
    # Start Kafka
    nohup "${KAFKA_HOME}/bin/kafka-server-start.sh" \
        "${KAFKA_HOME}/config/kraft/server.properties" \
        > "${LOG_DIR}/kafka.log" 2>&1 &
    
    echo $! > "${PID_DIR}/kafka.pid"
    
    # Wait for Kafka to start
    log_info "Waiting for Kafka to start..."
    sleep 10
    
    if pgrep -f "kafka.Kafka" > /dev/null; then
        log_success "Kafka started (PID: $(cat ${PID_DIR}/kafka.pid))"
    else
        log_error "Failed to start Kafka. Check ${LOG_DIR}/kafka.log"
        return 1
    fi
}

stop_kafka() {
    log_info "Stopping Kafka..."
    
    if [[ -f "${PID_DIR}/kafka.pid" ]]; then
        kill -SIGTERM "$(cat ${PID_DIR}/kafka.pid)" 2>/dev/null || true
        rm -f "${PID_DIR}/kafka.pid"
    fi
    
    # Also try to kill by process name
    pkill -f "kafka.Kafka" 2>/dev/null || true
    
    log_success "Kafka stopped"
}

create_topic() {
    log_info "Creating Kafka topic: ${KAFKA_TOPIC}..."
    
    "${KAFKA_HOME}/bin/kafka-topics.sh" --create \
        --topic "${KAFKA_TOPIC}" \
        --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists 2>/dev/null || true
    
    log_success "Topic '${KAFKA_TOPIC}' ready"
}

#===============================================================================
# MLflow Server
#===============================================================================

start_mlflow() {
    log_info "Starting MLflow server..."
    
    if pgrep -f "mlflow server" > /dev/null; then
        log_warning "MLflow server is already running"
        return 0
    fi
    
    nohup mlflow server \
        --host 0.0.0.0 \
        --port 5000 \
        --backend-store-uri "sqlite:///${DATA_DIR}/mlflow.db" \
        --default-artifact-root "s3://${S3_BUCKET}/mlflow-artifacts/" \
        > "${LOG_DIR}/mlflow.log" 2>&1 &
    
    echo $! > "${PID_DIR}/mlflow.pid"
    
    sleep 3
    
    if pgrep -f "mlflow server" > /dev/null; then
        log_success "MLflow server started at http://localhost:5000"
    else
        log_error "Failed to start MLflow. Check ${LOG_DIR}/mlflow.log"
        return 1
    fi
}

stop_mlflow() {
    log_info "Stopping MLflow server..."
    
    if [[ -f "${PID_DIR}/mlflow.pid" ]]; then
        kill -SIGTERM "$(cat ${PID_DIR}/mlflow.pid)" 2>/dev/null || true
        rm -f "${PID_DIR}/mlflow.pid"
    fi
    
    pkill -f "mlflow server" 2>/dev/null || true
    
    log_success "MLflow server stopped"
}

#===============================================================================
# Producer Management
#===============================================================================

start_producer() {
    log_info "Starting Kafka producer (${ORDERS_PER_SECOND} orders/sec)..."
    
    if [[ -f "${PID_DIR}/producer.pid" ]] && kill -0 "$(cat ${PID_DIR}/producer.pid)" 2>/dev/null; then
        log_warning "Producer is already running"
        return 0
    fi
    
    cd "${SCRIPT_DIR}"
    
    nohup python3 kafka_producer.py \
        --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS}" \
        --topic "${KAFKA_TOPIC}" \
        --rate "${ORDERS_PER_SECOND}" \
        > "${LOG_DIR}/producer.log" 2>&1 &
    
    echo $! > "${PID_DIR}/producer.pid"
    
    sleep 2
    
    if kill -0 "$(cat ${PID_DIR}/producer.pid)" 2>/dev/null; then
        log_success "Producer started (PID: $(cat ${PID_DIR}/producer.pid))"
    else
        log_error "Failed to start producer. Check ${LOG_DIR}/producer.log"
        return 1
    fi
}

stop_producer() {
    log_info "Stopping producer..."
    
    if [[ -f "${PID_DIR}/producer.pid" ]]; then
        kill -SIGTERM "$(cat ${PID_DIR}/producer.pid)" 2>/dev/null || true
        rm -f "${PID_DIR}/producer.pid"
    fi
    
    pkill -f "kafka_producer.py" 2>/dev/null || true
    
    log_success "Producer stopped"
}

#===============================================================================
# Consumer Management
#===============================================================================

start_consumer() {
    log_info "Starting Kafka consumer with S3 sink..."
    
    if [[ -f "${PID_DIR}/consumer.pid" ]] && kill -0 "$(cat ${PID_DIR}/consumer.pid)" 2>/dev/null; then
        log_warning "Consumer is already running"
        return 0
    fi
    
    cd "${SCRIPT_DIR}"
    
    nohup python3 kafka_consumer.py \
        --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS}" \
        --topic "${KAFKA_TOPIC}" \
        --s3-bucket "${S3_BUCKET}" \
        --s3-prefix "${S3_DATA_PREFIX}" \
        --aws-region "${AWS_REGION}" \
        --backup-dir "${DATA_DIR}/backup" \
        --batch-size "${BATCH_SIZE}" \
        --batch-timeout "${BATCH_TIMEOUT}" \
        > "${LOG_DIR}/consumer.log" 2>&1 &
    
    echo $! > "${PID_DIR}/consumer.pid"
    
    sleep 2
    
    if kill -0 "$(cat ${PID_DIR}/consumer.pid)" 2>/dev/null; then
        log_success "Consumer started (PID: $(cat ${PID_DIR}/consumer.pid))"
    else
        log_error "Failed to start consumer. Check ${LOG_DIR}/consumer.log"
        return 1
    fi
}

stop_consumer() {
    log_info "Stopping consumer..."
    
    if [[ -f "${PID_DIR}/consumer.pid" ]]; then
        kill -SIGTERM "$(cat ${PID_DIR}/consumer.pid)" 2>/dev/null || true
        rm -f "${PID_DIR}/consumer.pid"
    fi
    
    pkill -f "kafka_consumer.py" 2>/dev/null || true
    
    log_success "Consumer stopped"
}

#===============================================================================
# Model Training
#===============================================================================

run_training() {
    log_info "Running model training..."
    
    cd "${SCRIPT_DIR}"
    
    python3 train_model.py \
        --s3-bucket "${S3_BUCKET}" \
        --s3-data-prefix "${S3_DATA_PREFIX}" \
        --s3-model-prefix "${S3_MODEL_PREFIX}" \
        --aws-region "${AWS_REGION}" \
        --mlflow-uri "${MLFLOW_TRACKING_URI}" \
        --experiment "${MLFLOW_EXPERIMENT}" \
        --local-backup "${DATA_DIR}/backup" \
        --strategy weighted \
        2>&1 | tee "${LOG_DIR}/training.log"
    
    log_success "Model training complete"
}

#===============================================================================
# Status Check
#===============================================================================

check_status() {
    echo ""
    echo "==============================================="
    echo "       ZOMATO STREAMING PIPELINE STATUS        "
    echo "==============================================="
    echo ""
    
    # Kafka
    if pgrep -f "kafka.Kafka" > /dev/null; then
        echo -e "Kafka Broker:    ${GREEN}RUNNING${NC}"
    else
        echo -e "Kafka Broker:    ${RED}STOPPED${NC}"
    fi
    
    # MLflow
    if pgrep -f "mlflow server" > /dev/null; then
        echo -e "MLflow Server:   ${GREEN}RUNNING${NC} (http://localhost:5000)"
    else
        echo -e "MLflow Server:   ${RED}STOPPED${NC}"
    fi
    
    # Producer
    if [[ -f "${PID_DIR}/producer.pid" ]] && kill -0 "$(cat ${PID_DIR}/producer.pid)" 2>/dev/null; then
        echo -e "Kafka Producer:  ${GREEN}RUNNING${NC} (PID: $(cat ${PID_DIR}/producer.pid))"
    else
        echo -e "Kafka Producer:  ${RED}STOPPED${NC}"
    fi
    
    # Consumer
    if [[ -f "${PID_DIR}/consumer.pid" ]] && kill -0 "$(cat ${PID_DIR}/consumer.pid)" 2>/dev/null; then
        echo -e "Kafka Consumer:  ${GREEN}RUNNING${NC} (PID: $(cat ${PID_DIR}/consumer.pid))"
    else
        echo -e "Kafka Consumer:  ${RED}STOPPED${NC}"
    fi
    
    echo ""
    echo "Configuration:"
    echo "  Kafka Servers:  ${KAFKA_BOOTSTRAP_SERVERS}"
    echo "  Kafka Topic:    ${KAFKA_TOPIC}"
    echo "  S3 Bucket:      ${S3_BUCKET}"
    echo "  MLflow URI:     ${MLFLOW_TRACKING_URI}"
    echo ""
    
    # Check recent logs
    echo "Recent Activity (last 5 lines):"
    echo "---"
    for log in producer consumer; do
        if [[ -f "${LOG_DIR}/${log}.log" ]]; then
            echo "[${log}]"
            tail -2 "${LOG_DIR}/${log}.log" 2>/dev/null || echo "  (no logs)"
        fi
    done
    echo ""
}

#===============================================================================
# Full Pipeline Control
#===============================================================================

start_all() {
    log_info "Starting full pipeline..."
    echo ""
    
    create_directories
    
    # Start Kafka (if not using external)
    if [[ "${SKIP_KAFKA}" != "true" ]]; then
        start_kafka_kraft
        create_topic
    fi
    
    # Start MLflow
    start_mlflow
    
    # Start Producer (background)
    start_producer
    
    # Start Consumer
    start_consumer
    
    echo ""
    log_success "Pipeline started successfully!"
    echo ""
    check_status
}

stop_all() {
    log_info "Stopping full pipeline..."
    echo ""
    
    stop_consumer
    stop_producer
    stop_mlflow
    
    if [[ "${SKIP_KAFKA}" != "true" ]]; then
        stop_kafka
    fi
    
    echo ""
    log_success "Pipeline stopped"
}

#===============================================================================
# Main
#===============================================================================

print_usage() {
    echo "Usage: $0 {start|stop|restart|status|train|logs}"
    echo ""
    echo "Commands:"
    echo "  start    - Start full pipeline (Kafka, MLflow, Producer, Consumer)"
    echo "  stop     - Stop all components"
    echo "  restart  - Stop and start all components"
    echo "  status   - Show status of all components"
    echo "  train    - Run model training"
    echo "  logs     - Tail logs from all components"
    echo ""
    echo "Individual components:"
    echo "  start-kafka     - Start Kafka broker only"
    echo "  start-mlflow    - Start MLflow server only"
    echo "  start-producer  - Start producer only"
    echo "  start-consumer  - Start consumer only"
    echo ""
    echo "Environment variables:"
    echo "  KAFKA_HOME              - Kafka installation directory"
    echo "  KAFKA_BOOTSTRAP_SERVERS - Kafka servers (default: localhost:9092)"
    echo "  S3_BUCKET               - S3 bucket name (default: zomatoovertomato)"
    echo "  ORDERS_PER_SECOND       - Producer rate (default: 10)"
    echo "  SKIP_KAFKA              - Skip Kafka start (use external)"
    echo ""
}

case "$1" in
    start)
        start_all
        ;;
    stop)
        stop_all
        ;;
    restart)
        stop_all
        sleep 3
        start_all
        ;;
    status)
        check_status
        ;;
    train)
        run_training
        ;;
    logs)
        tail -f "${LOG_DIR}"/*.log
        ;;
    start-kafka)
        create_directories
        start_kafka_kraft
        create_topic
        ;;
    start-mlflow)
        create_directories
        start_mlflow
        ;;
    start-producer)
        create_directories
        start_producer
        ;;
    start-consumer)
        create_directories
        start_consumer
        ;;
    stop-kafka)
        stop_kafka
        ;;
    stop-mlflow)
        stop_mlflow
        ;;
    stop-producer)
        stop_producer
        ;;
    stop-consumer)
        stop_consumer
        ;;
    *)
        print_usage
        exit 1
        ;;
esac
