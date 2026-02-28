#!/usr/bin/env python3
"""
Kafka Producer for Zomato Order Streaming
Streams synthetic food delivery orders to Kafka topic 'zomato-orders'
Configurable rate: ~10 orders/second (suitable for t3.micro)
"""

import os
import sys
import json
import time
import math
import random
import signal
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker

# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'zomato-orders')
ORDERS_PER_SECOND = float(os.getenv('ORDERS_PER_SECOND', '10'))

# Initialize Faker for Indian locale
fake = Faker('en_IN')

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('kafka_producer.log')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS (from generate_dataset.py)
# ============================================================================

INDIAN_CITIES = {
    'Mumbai': {'lat_min': 18.89, 'lat_max': 19.27, 'lng_min': 72.77, 'lng_max': 72.98},
    'Delhi': {'lat_min': 28.40, 'lat_max': 28.88, 'lng_min': 76.84, 'lng_max': 77.35},
    'Bangalore': {'lat_min': 12.85, 'lat_max': 13.14, 'lng_min': 77.46, 'lng_max': 77.78},
    'Hyderabad': {'lat_min': 17.29, 'lat_max': 17.55, 'lng_min': 78.35, 'lng_max': 78.60},
    'Pune': {'lat_min': 18.43, 'lat_max': 18.62, 'lng_min': 73.73, 'lng_max': 73.98},
    'Chennai': {'lat_min': 12.92, 'lat_max': 13.23, 'lng_min': 80.15, 'lng_max': 80.32},
}

CUISINE_TYPES = ['North Indian', 'South Indian', 'Chinese', 'Fast Food', 'Biryani', 'Pizza', 'Beverages']
ITEM_CATEGORIES = ['Main Course', 'Biryani', 'Snacks', 'Beverage', 'Dessert', 'Combo']

ITEM_COMPLEXITY = {
    'Beverage': (0.2, 0.4),
    'Snacks': (0.3, 0.6),
    'Dessert': (0.4, 0.7),
    'Main Course': (0.7, 1.2),
    'Biryani': (1.2, 1.6),
    'Combo': (1.4, 1.8),
}

WEATHER_CONDITIONS = ['Clear', 'Rainy', 'Cloudy', 'Stormy']
WEATHER_WEIGHTS = [0.55, 0.20, 0.20, 0.05]

DAYS_OF_WEEK = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

RESTAURANT_PREFIXES = [
    'Royal', 'Golden', 'Spice', 'Tandoori', 'Delhi', 'Mumbai', 'Punjabi',
    'South', 'Hyderabadi', 'Bangalore', 'Chennai', 'Grand', 'Classic',
    'Authentic', 'The Great', 'New', 'Shri', 'Raj', 'Annapurna', 'Sagar',
    'Paradise', 'Nawab', 'Desi', 'Urban', 'Street', 'Chef', 'Kitchen'
]

RESTAURANT_SUFFIXES = [
    'Kitchen', 'House', 'Palace', 'Corner', 'Express', 'Delight', 'Point',
    'Junction', 'Hub', 'Grill', 'Bites', 'Treats', 'Cafe', 'Dhaba',
    'Restaurant', 'Foods', 'Eatery', 'Bistro', 'Diner', 'Hut', 'Stop'
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate haversine distance between two points in meters."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)
    
    a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


def generate_point_at_distance(lat: float, lng: float, 
                                min_dist_m: float, max_dist_m: float) -> Tuple[float, float]:
    """Generate a random point at specified distance range from origin."""
    bearing = random.uniform(0, 2 * math.pi)
    distance = random.uniform(min_dist_m, max_dist_m)
    R = 6371000
    
    lat_rad = math.radians(lat)
    lng_rad = math.radians(lng)
    
    new_lat_rad = math.asin(
        math.sin(lat_rad) * math.cos(distance/R) +
        math.cos(lat_rad) * math.sin(distance/R) * math.cos(bearing)
    )
    
    new_lng_rad = lng_rad + math.atan2(
        math.sin(bearing) * math.sin(distance/R) * math.cos(lat_rad),
        math.cos(distance/R) - math.sin(lat_rad) * math.sin(new_lat_rad)
    )
    
    return round(math.degrees(new_lat_rad), 6), round(math.degrees(new_lng_rad), 6)


def is_peak_hour(dt: datetime) -> bool:
    """Check if timestamp falls in peak hours (12pm-2pm or 7pm-10pm)."""
    hour = dt.hour
    return (12 <= hour < 14) or (19 <= hour < 22)


def calculate_actual_kpt(complexity: float, kitchen_load: float,
                         concurrent_orders: int, is_peak: bool) -> int:
    """Calculate actual Kitchen Prep Time in seconds."""
    import numpy as np
    base_kpt = np.random.normal(1200, 300)
    complexity_factor = 0.5 + (complexity * 0.5)
    load_factor = 1 + (kitchen_load * 0.5)
    concurrent_factor = 1 + (concurrent_orders - 1) * 0.05
    peak_factor = 1.15 if is_peak else 1.0
    
    kpt = base_kpt * complexity_factor * load_factor * concurrent_factor * peak_factor
    return int(max(480, min(2700, kpt)))


def generate_label_confidence(is_rider_influenced: bool) -> float:
    """Generate label confidence score (0.4 to 1.0)."""
    if is_rider_influenced:
        return round(random.uniform(0.4, 0.7), 2)
    return round(random.uniform(0.7, 1.0), 2)


# ============================================================================
# MERCHANT POOL (Pre-generated for consistency)
# ============================================================================

class MerchantPool:
    """Pool of pre-generated merchants for consistent data."""
    
    def __init__(self, num_merchants: int = 500):
        self.merchants = []
        random.seed(42)  # Consistent merchants
        
        for i in range(num_merchants):
            city = random.choice(list(INDIAN_CITIES.keys()))
            bounds = INDIAN_CITIES[city]
            
            lat = random.uniform(bounds['lat_min'], bounds['lat_max'])
            lng = random.uniform(bounds['lng_min'], bounds['lng_max'])
            
            self.merchants.append({
                'merchant_id': f"MER{str(i+1).zfill(5)}",
                'restaurant_name': f"{random.choice(RESTAURANT_PREFIXES)} {random.choice(RESTAURANT_SUFFIXES)}",
                'city': city,
                'restaurant_lat': round(lat, 6),
                'restaurant_lng': round(lng, 6),
                'cuisine_type': random.choice(CUISINE_TYPES),
            })
        
        # Reset random seed for order generation
        random.seed()
    
    def get_random_merchant(self) -> Dict:
        return random.choice(self.merchants)


# ============================================================================
# ORDER GENERATOR
# ============================================================================

class OrderGenerator:
    """Generates single orders for streaming."""
    
    def __init__(self):
        self.merchant_pool = MerchantPool()
        self.order_counter = 0
        import numpy as np
        np.random.seed()  # Random seed for order variety
    
    def generate_order(self) -> Dict:
        """Generate a single order with all 25 fields."""
        self.order_counter += 1
        merchant = self.merchant_pool.get_random_merchant()
        
        # Current timestamp (real-time simulation)
        order_confirmed_at = datetime.now()
        
        # Prep time between 8 and 45 minutes
        prep_time_minutes = random.randint(8, 45)
        for_timestamp = order_confirmed_at + timedelta(minutes=prep_time_minutes)
        
        # Rider arrival: around FOR time (+/- few minutes)
        rider_arrival_offset = random.randint(-5, 10)
        rider_arrived_at = for_timestamp + timedelta(minutes=rider_arrival_offset)
        
        # Rider info
        rider_id = f"RDR{random.randint(1, 10000):05d}"
        
        # 35% chance rider is nearby (rider-influenced FOR)
        is_for_rider_influenced = random.random() < 0.35
        
        if is_for_rider_influenced:
            rider_lat, rider_lng = generate_point_at_distance(
                merchant['restaurant_lat'], merchant['restaurant_lng'], 0, 200
            )
        else:
            rider_lat, rider_lng = generate_point_at_distance(
                merchant['restaurant_lat'], merchant['restaurant_lng'], 500, 3000
            )
        
        # Order items
        num_items = random.randint(1, 6)
        item_category = random.choice(ITEM_CATEGORIES)
        complexity_range = ITEM_COMPLEXITY[item_category]
        item_complexity_score = round(random.uniform(*complexity_range), 2)
        
        # Kitchen load
        concurrent_orders = random.randint(1, 8)
        kitchen_load_index = round(random.uniform(0.1, 1.0), 2)
        is_peak = is_peak_hour(order_confirmed_at)
        day_of_week = DAYS_OF_WEEK[order_confirmed_at.weekday()]
        is_weekend = order_confirmed_at.weekday() >= 5
        weather = random.choices(WEATHER_CONDITIONS, weights=WEATHER_WEIGHTS, k=1)[0]
        
        # Labels
        actual_kpt_seconds = calculate_actual_kpt(
            item_complexity_score, kitchen_load_index, concurrent_orders, is_peak
        )
        label_confidence = generate_label_confidence(is_for_rider_influenced)
        
        return {
            # Order & Merchant Info
            'order_id': f"ORD{str(self.order_counter).zfill(8)}",
            'merchant_id': merchant['merchant_id'],
            'restaurant_name': merchant['restaurant_name'],
            'city': merchant['city'],
            'cuisine_type': merchant['cuisine_type'],
            'restaurant_lat': merchant['restaurant_lat'],
            'restaurant_lng': merchant['restaurant_lng'],
            
            # Timestamps
            'order_confirmed_at': order_confirmed_at.strftime('%Y-%m-%d %H:%M:%S'),
            'for_timestamp': for_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'rider_arrived_at': rider_arrived_at.strftime('%Y-%m-%d %H:%M:%S'),
            
            # Rider Info
            'rider_id': rider_id,
            'rider_lat': rider_lat,
            'rider_lng': rider_lng,
            
            # Order Items
            'num_items': num_items,
            'item_category': item_category,
            'item_complexity_score': item_complexity_score,
            
            # Kitchen Load
            'concurrent_orders': concurrent_orders,
            'kitchen_load_index': kitchen_load_index,
            'is_peak_hour': is_peak,
            'day_of_week': day_of_week,
            'is_weekend': is_weekend,
            'weather_condition': weather,
            
            # Labels
            'actual_kpt_seconds': actual_kpt_seconds,
            'is_for_rider_influenced': is_for_rider_influenced,
            'label_confidence': label_confidence,
            
            # Metadata
            'stream_timestamp': datetime.now().isoformat(),
        }


# ============================================================================
# KAFKA PRODUCER
# ============================================================================

class ZomatoKafkaProducer:
    """Kafka producer for streaming Zomato orders."""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.order_generator = OrderGenerator()
        self.running = False
        self.orders_sent = 0
        self.errors = 0
        
        logger.info(f"Connecting to Kafka at {bootstrap_servers}...")
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,
            linger_ms=10,  # Small batching for low latency
            batch_size=16384,
        )
        
        logger.info(f"Connected to Kafka. Topic: {topic}")
    
    def on_send_success(self, record_metadata):
        """Callback for successful send."""
        self.orders_sent += 1
        if self.orders_sent % 100 == 0:
            logger.info(f"Orders sent: {self.orders_sent} | Topic: {record_metadata.topic} | Partition: {record_metadata.partition}")
    
    def on_send_error(self, excp):
        """Callback for send error."""
        self.errors += 1
        logger.error(f"Error sending message: {excp}")
    
    def send_order(self, order: Dict):
        """Send a single order to Kafka."""
        try:
            future = self.producer.send(
                self.topic,
                key=order['order_id'],
                value=order
            )
            future.add_callback(self.on_send_success)
            future.add_errback(self.on_send_error)
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            self.errors += 1
    
    def start_streaming(self, orders_per_second: float = 10, max_orders: Optional[int] = None):
        """Start streaming orders at specified rate."""
        self.running = True
        interval = 1.0 / orders_per_second
        
        logger.info(f"Starting to stream at {orders_per_second} orders/second...")
        
        start_time = time.time()
        order_count = 0
        
        try:
            while self.running:
                if max_orders and order_count >= max_orders:
                    logger.info(f"Reached max orders limit: {max_orders}")
                    break
                
                loop_start = time.time()
                
                # Generate and send order
                order = self.order_generator.generate_order()
                self.send_order(order)
                order_count += 1
                
                # Rate limiting
                elapsed = time.time() - loop_start
                sleep_time = max(0, interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
                # Log stats every 10 seconds
                if order_count % (int(orders_per_second) * 10) == 0:
                    elapsed_total = time.time() - start_time
                    actual_rate = order_count / elapsed_total
                    logger.info(f"Stats: {order_count} orders in {elapsed_total:.1f}s | Rate: {actual_rate:.2f}/s | Errors: {self.errors}")
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the producer gracefully."""
        self.running = False
        logger.info("Flushing remaining messages...")
        self.producer.flush()
        self.producer.close()
        logger.info(f"Producer stopped. Total orders sent: {self.orders_sent}, Errors: {self.errors}")
    
    def get_stats(self) -> Dict:
        """Get producer statistics."""
        return {
            'orders_sent': self.orders_sent,
            'errors': self.errors,
            'running': self.running,
        }


# ============================================================================
# SIGNAL HANDLERS
# ============================================================================

producer_instance: Optional[ZomatoKafkaProducer] = None


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down...")
    if producer_instance:
        producer_instance.stop()
    sys.exit(0)


# ============================================================================
# MAIN
# ============================================================================

def main():
    global producer_instance
    
    parser = argparse.ArgumentParser(description='Kafka Producer for Zomato Orders')
    parser.add_argument('--bootstrap-servers', default=KAFKA_BOOTSTRAP_SERVERS,
                        help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default=KAFKA_TOPIC,
                        help='Kafka topic (default: zomato-orders)')
    parser.add_argument('--rate', type=float, default=ORDERS_PER_SECOND,
                        help='Orders per second (default: 10)')
    parser.add_argument('--max-orders', type=int, default=None,
                        help='Maximum orders to send (default: unlimited)')
    
    args = parser.parse_args()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("=" * 60)
    logger.info("Zomato Kafka Order Producer")
    logger.info("=" * 60)
    logger.info(f"Bootstrap Servers: {args.bootstrap_servers}")
    logger.info(f"Topic: {args.topic}")
    logger.info(f"Rate: {args.rate} orders/second")
    logger.info(f"Max Orders: {args.max_orders or 'Unlimited'}")
    logger.info("=" * 60)
    
    producer_instance = ZomatoKafkaProducer(args.bootstrap_servers, args.topic)
    producer_instance.start_streaming(args.rate, args.max_orders)


if __name__ == '__main__':
    main()
