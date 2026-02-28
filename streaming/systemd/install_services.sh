#!/bin/bash
#===============================================================================
# Systemd Services Installation Script
# Run this on EC2 to install all services
#===============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICES=("kafka" "zomato-producer" "zomato-consumer" "zomato-mlflow")

echo "Installing Zomato Pipeline Systemd Services..."
echo ""

# Create log directory
sudo mkdir -p /var/log/zomato
sudo chown ec2-user:ec2-user /var/log/zomato

# Copy service files
for service in "${SERVICES[@]}"; do
    echo "Installing ${service}.service..."
    sudo cp "${SCRIPT_DIR}/${service}.service" /etc/systemd/system/
    sudo chmod 644 "/etc/systemd/system/${service}.service"
done

# Reload systemd
echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

# Enable services (start on boot)
echo "Enabling services..."
for service in "${SERVICES[@]}"; do
    sudo systemctl enable "${service}.service"
done

echo ""
echo "Services installed successfully!"
echo ""
echo "Usage:"
echo "  sudo systemctl start kafka              # Start Kafka"
echo "  sudo systemctl start zomato-producer    # Start Producer"
echo "  sudo systemctl start zomato-consumer    # Start Consumer"
echo "  sudo systemctl start zomato-mlflow      # Start MLflow"
echo ""
echo "  sudo systemctl status kafka             # Check status"
echo "  sudo journalctl -u zomato-producer -f   # View logs"
echo ""
echo "Start all services:"
echo "  sudo systemctl start kafka && sleep 10 && \\"
echo "  sudo systemctl start zomato-mlflow && \\"
echo "  sudo systemctl start zomato-producer && \\"
echo "  sudo systemctl start zomato-consumer"
