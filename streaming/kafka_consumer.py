#!/usr/bin/env python3
"""
Kafka Consumer with S3 Sink for Zomato Orders
Consumes from 'zomato-orders' topic, batches records, writes Parquet to S3 + local CSV backup.
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
import threading
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

import boto3
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from botocore.exceptions import ClientError

# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'zomato-orders')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'zomato-consumer-group')

S3_BUCKET = os.getenv('S3_BUCKET', 'zomatoovertomato')
S3_PREFIX = os.getenv('S3_PREFIX', 'raw-orders')
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
BATCH_TIMEOUT_SECONDS = int(os.getenv('BATCH_TIMEOUT_SECONDS', '60'))

LOCAL_BACKUP_DIR = os.getenv('LOCAL_BACKUP_DIR', './data/backup')

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('kafka_consumer.log')
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# S3 HANDLER
# ============================================================================

class S3Handler:
    """Handles S3 operations for writing Parquet files."""
    
    def __init__(self, bucket: str, prefix: str, region: str):
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        
        logger.info(f"Initializing S3 client for bucket: {bucket}")
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3', region_name=region)
        
        # Verify bucket access
        try:
            self.s3_client.head_bucket(Bucket=bucket)
            logger.info(f"Successfully connected to S3 bucket: {bucket}")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404':
                logger.warning(f"Bucket {bucket} does not exist, will create on first write")
            elif error_code == '403':
                logger.error(f"Access denied to bucket {bucket}. Check AWS credentials.")
            else:
                logger.error(f"Error accessing bucket: {e}")
    
    def write_parquet(self, df: pd.DataFrame, partition_key: str) -> str:
        """Write DataFrame as Parquet to S3."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"orders_{partition_key}_{timestamp}.parquet"
        
        # Create partitioned path: year=YYYY/month=MM/day=DD/
        now = datetime.now()
        partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        s3_key = f"{self.prefix}/{partition_path}/{filename}"
        
        # Write to temp file first
        temp_path = f"/tmp/{filename}"
        df.to_parquet(temp_path, index=False, compression='snappy')
        
        try:
            self.s3_client.upload_file(temp_path, self.bucket, s3_key)
            s3_uri = f"s3://{self.bucket}/{s3_key}"
            logger.info(f"Uploaded to S3: {s3_uri} ({len(df)} records)")
            
            # Cleanup temp file
            os.remove(temp_path)
            
            return s3_uri
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise
    
    def list_files(self, prefix: Optional[str] = None) -> List[str]:
        """List files in S3 bucket."""
        prefix = prefix or self.prefix
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            logger.error(f"Failed to list S3 files: {e}")
            return []


# ============================================================================
# LOCAL BACKUP HANDLER
# ============================================================================

class LocalBackupHandler:
    """Handles local CSV backup."""
    
    def __init__(self, backup_dir: str):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Daily backup file
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        self.backup_file = self.backup_dir / f"orders_backup_{self.current_date}.csv"
        
        logger.info(f"Local backup directory: {self.backup_dir}")
    
    def append_records(self, df: pd.DataFrame):
        """Append records to local CSV backup."""
        # Check if date changed (new day)
        current_date = datetime.now().strftime('%Y-%m-%d')
        if current_date != self.current_date:
            self.current_date = current_date
            self.backup_file = self.backup_dir / f"orders_backup_{self.current_date}.csv"
        
        # Append to CSV (create with header if new file)
        file_exists = self.backup_file.exists()
        df.to_csv(self.backup_file, mode='a', header=not file_exists, index=False)
        logger.debug(f"Appended {len(df)} records to {self.backup_file}")
    
    def get_backup_stats(self) -> Dict:
        """Get backup statistics."""
        total_size = sum(f.stat().st_size for f in self.backup_dir.glob('*.csv'))
        num_files = len(list(self.backup_dir.glob('*.csv')))
        return {
            'backup_dir': str(self.backup_dir),
            'num_files': num_files,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }


# ============================================================================
# BATCH PROCESSOR
# ============================================================================

class BatchProcessor:
    """Processes batches of records and writes to S3 + local backup."""
    
    def __init__(self, s3_handler: S3Handler, backup_handler: LocalBackupHandler,
                 batch_size: int, batch_timeout: int):
        self.s3_handler = s3_handler
        self.backup_handler = backup_handler
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        self.batch: List[Dict] = []
        self.last_flush_time = time.time()
        self.batch_count = 0
        self.total_records = 0
        self.lock = threading.Lock()
    
    def add_record(self, record: Dict):
        """Add a record to the batch."""
        with self.lock:
            self.batch.append(record)
            
            # Check if batch should be flushed
            if len(self.batch) >= self.batch_size:
                self._flush_batch("size_limit")
    
    def check_timeout_flush(self):
        """Check if batch should be flushed due to timeout."""
        with self.lock:
            elapsed = time.time() - self.last_flush_time
            if elapsed >= self.batch_timeout and len(self.batch) > 0:
                self._flush_batch("timeout")
    
    def _flush_batch(self, reason: str):
        """Flush the current batch to S3 and local backup."""
        if not self.batch:
            return
        
        self.batch_count += 1
        batch_size = len(self.batch)
        
        logger.info(f"Flushing batch #{self.batch_count} ({batch_size} records, reason: {reason})")
        
        # Convert to DataFrame
        df = pd.DataFrame(self.batch)
        
        # Generate partition key
        partition_key = f"batch_{self.batch_count:06d}"
        
        try:
            # Write to S3
            s3_uri = self.s3_handler.write_parquet(df, partition_key)
            
            # Write to local backup
            self.backup_handler.append_records(df)
            
            self.total_records += batch_size
            logger.info(f"Batch #{self.batch_count} flushed successfully. Total records: {self.total_records}")
        
        except Exception as e:
            logger.error(f"Error flushing batch: {e}")
            # Still write to local backup as fallback
            try:
                self.backup_handler.append_records(df)
                logger.info("Saved to local backup as fallback")
            except Exception as e2:
                logger.error(f"Local backup also failed: {e2}")
        
        # Clear batch
        self.batch = []
        self.last_flush_time = time.time()
    
    def flush_remaining(self):
        """Flush any remaining records in the batch."""
        with self.lock:
            if self.batch:
                self._flush_batch("shutdown")
    
    def get_stats(self) -> Dict:
        """Get processor statistics."""
        return {
            'batch_count': self.batch_count,
            'total_records': self.total_records,
            'current_batch_size': len(self.batch),
            'last_flush_seconds_ago': round(time.time() - self.last_flush_time, 1)
        }


# ============================================================================
# KAFKA CONSUMER
# ============================================================================

class ZomatoKafkaConsumer:
    """Kafka consumer for Zomato orders with S3 sink."""
    
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str,
                 s3_bucket: str, s3_prefix: str, aws_region: str,
                 backup_dir: str, batch_size: int, batch_timeout: int):
        
        self.topic = topic
        self.running = False
        self.messages_consumed = 0
        self.errors = 0
        
        # Initialize handlers
        self.s3_handler = S3Handler(s3_bucket, s3_prefix, aws_region)
        self.backup_handler = LocalBackupHandler(backup_dir)
        self.batch_processor = BatchProcessor(
            self.s3_handler, self.backup_handler, batch_size, batch_timeout
        )
        
        logger.info(f"Connecting to Kafka at {bootstrap_servers}...")
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(','),
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            max_poll_records=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        
        logger.info(f"Connected to Kafka. Topic: {topic}, Group: {group_id}")
        
        # Timeout checker thread
        self.timeout_checker_thread = None
    
    def _timeout_checker(self):
        """Background thread to check for timeout-based batch flush."""
        while self.running:
            time.sleep(5)  # Check every 5 seconds
            self.batch_processor.check_timeout_flush()
    
    def start_consuming(self, max_messages: Optional[int] = None):
        """Start consuming messages from Kafka."""
        self.running = True
        
        # Start timeout checker thread
        self.timeout_checker_thread = threading.Thread(target=self._timeout_checker, daemon=True)
        self.timeout_checker_thread.start()
        
        logger.info("Starting to consume messages...")
        start_time = time.time()
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                if max_messages and self.messages_consumed >= max_messages:
                    logger.info(f"Reached max messages limit: {max_messages}")
                    break
                
                try:
                    # Process message
                    record = message.value
                    self.batch_processor.add_record(record)
                    self.messages_consumed += 1
                    
                    # Log stats every 100 messages
                    if self.messages_consumed % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = self.messages_consumed / elapsed
                        stats = self.batch_processor.get_stats()
                        logger.info(
                            f"Consumed: {self.messages_consumed} | "
                            f"Rate: {rate:.2f}/s | "
                            f"Batches: {stats['batch_count']} | "
                            f"Total Written: {stats['total_records']}"
                        )
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.errors += 1
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the consumer gracefully."""
        self.running = False
        
        logger.info("Flushing remaining records...")
        self.batch_processor.flush_remaining()
        
        logger.info("Closing consumer...")
        self.consumer.close()
        
        stats = self.batch_processor.get_stats()
        backup_stats = self.backup_handler.get_backup_stats()
        
        logger.info("=" * 60)
        logger.info("Consumer Statistics:")
        logger.info(f"  Messages Consumed: {self.messages_consumed}")
        logger.info(f"  Batches Written: {stats['batch_count']}")
        logger.info(f"  Total Records Written: {stats['total_records']}")
        logger.info(f"  Errors: {self.errors}")
        logger.info(f"  Local Backup: {backup_stats['num_files']} files, {backup_stats['total_size_mb']} MB")
        logger.info("=" * 60)
    
    def get_stats(self) -> Dict:
        """Get consumer statistics."""
        return {
            'messages_consumed': self.messages_consumed,
            'errors': self.errors,
            'running': self.running,
            'batch_stats': self.batch_processor.get_stats(),
            'backup_stats': self.backup_handler.get_backup_stats()
        }


# ============================================================================
# SIGNAL HANDLERS
# ============================================================================

consumer_instance: Optional[ZomatoKafkaConsumer] = None


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down...")
    if consumer_instance:
        consumer_instance.stop()
    sys.exit(0)


# ============================================================================
# MAIN
# ============================================================================

def main():
    global consumer_instance
    
    parser = argparse.ArgumentParser(description='Kafka Consumer with S3 Sink for Zomato Orders')
    parser.add_argument('--bootstrap-servers', default=KAFKA_BOOTSTRAP_SERVERS,
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', default=KAFKA_TOPIC,
                        help='Kafka topic')
    parser.add_argument('--group-id', default=KAFKA_GROUP_ID,
                        help='Kafka consumer group ID')
    parser.add_argument('--s3-bucket', default=S3_BUCKET,
                        help='S3 bucket name')
    parser.add_argument('--s3-prefix', default=S3_PREFIX,
                        help='S3 key prefix')
    parser.add_argument('--aws-region', default=AWS_REGION,
                        help='AWS region')
    parser.add_argument('--backup-dir', default=LOCAL_BACKUP_DIR,
                        help='Local backup directory')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help='Batch size before flush')
    parser.add_argument('--batch-timeout', type=int, default=BATCH_TIMEOUT_SECONDS,
                        help='Batch timeout in seconds')
    parser.add_argument('--max-messages', type=int, default=None,
                        help='Maximum messages to consume')
    
    args = parser.parse_args()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("=" * 60)
    logger.info("Zomato Kafka Consumer with S3 Sink")
    logger.info("=" * 60)
    logger.info(f"Bootstrap Servers: {args.bootstrap_servers}")
    logger.info(f"Topic: {args.topic}")
    logger.info(f"Consumer Group: {args.group_id}")
    logger.info(f"S3 Bucket: {args.s3_bucket}")
    logger.info(f"S3 Prefix: {args.s3_prefix}")
    logger.info(f"Batch Size: {args.batch_size}")
    logger.info(f"Batch Timeout: {args.batch_timeout}s")
    logger.info(f"Local Backup: {args.backup_dir}")
    logger.info("=" * 60)
    
    consumer_instance = ZomatoKafkaConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        aws_region=args.aws_region,
        backup_dir=args.backup_dir,
        batch_size=args.batch_size,
        batch_timeout=args.batch_timeout
    )
    
    consumer_instance.start_consuming(args.max_messages)


if __name__ == '__main__':
    main()
