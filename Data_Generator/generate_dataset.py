#!/usr/bin/env python3
"""
Zomato-style Food Delivery Order Dataset Generator
Generates 100,000 rows simulating kitchen operations with FOR signal analysis.
"""

import random
import math
import uuid
from datetime import datetime, timedelta
from typing import Tuple, List, Dict
import numpy as np
import pandas as pd
from faker import Faker

# Initialize Faker for Indian locale
fake = Faker('en_IN')

# Set random seed for reproducibility
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)

# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

NUM_ROWS = 100_000
NUM_MERCHANTS = 500

# Indian cities with approximate lat/lng bounding boxes
INDIAN_CITIES = {
    'Mumbai': {'lat_min': 18.89, 'lat_max': 19.27, 'lng_min': 72.77, 'lng_max': 72.98},
    'Delhi': {'lat_min': 28.40, 'lat_max': 28.88, 'lng_min': 76.84, 'lng_max': 77.35},
    'Bangalore': {'lat_min': 12.85, 'lat_max': 13.14, 'lng_min': 77.46, 'lng_max': 77.78},
    'Hyderabad': {'lat_min': 17.29, 'lat_max': 17.55, 'lng_min': 78.35, 'lng_max': 78.60},
    'Pune': {'lat_min': 18.43, 'lat_max': 18.62, 'lng_min': 73.73, 'lng_max': 73.98},
    'Chennai': {'lat_min': 12.92, 'lat_max': 13.23, 'lng_min': 80.15, 'lng_max': 80.32},
}

CUISINE_TYPES = [
    'North Indian', 'South Indian', 'Chinese', 
    'Fast Food', 'Biryani', 'Pizza', 'Beverages'
]

ITEM_CATEGORIES = ['Main Course', 'Biryani', 'Snacks', 'Beverage', 'Dessert', 'Combo']

# Complexity scores by item category (range 0.2 - 1.8)
ITEM_COMPLEXITY = {
    'Beverage': (0.2, 0.4),
    'Snacks': (0.3, 0.6),
    'Dessert': (0.4, 0.7),
    'Main Course': (0.7, 1.2),
    'Biryani': (1.2, 1.6),
    'Combo': (1.4, 1.8),
}

WEATHER_CONDITIONS = ['Clear', 'Rainy', 'Cloudy', 'Stormy']
WEATHER_WEIGHTS = [0.55, 0.20, 0.20, 0.05]  # Clear is most common

DAYS_OF_WEEK = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# Restaurant name components for generation
RESTAURANT_PREFIXES = [
    'Royal', 'Golden', 'Spice', 'Tandoori', 'Delhi', 'Mumbai', 'Punjabi', 
    'South', 'Hyderabadi', 'Bangalore', 'Chennai', 'Grand', 'Classic',
    'Authentic', 'The Great', 'New', 'Shri', 'Raj', 'Annapurna', 'Sagar',
    'Paradise', 'Nawab', 'Desi', 'Urban', 'Street', 'Chef', 'Kitchen',
    'Masala', 'Mirchi', 'Tadka', 'Dhaba', 'Cafe', 'Biryani', 'Curry'
]

RESTAURANT_SUFFIXES = [
    'Kitchen', 'House', 'Palace', 'Corner', 'Express', 'Delight', 'Point',
    'Junction', 'Hub', 'Grill', 'Bites', 'Treats', 'Cafe', 'Dhaba',
    'Restaurant', 'Foods', 'Eatery', 'Bistro', 'Diner', 'Hut', 'Stop',
    'Place', 'Den', 'Lounge', 'Garden', 'Terrace', 'Zone', 'Central'
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_merchant_pool(num_merchants: int) -> List[Dict]:
    """Generate a pool of merchants with fixed attributes."""
    merchants = []
    for i in range(num_merchants):
        city = random.choice(list(INDIAN_CITIES.keys()))
        city_bounds = INDIAN_CITIES[city]
        
        lat = random.uniform(city_bounds['lat_min'], city_bounds['lat_max'])
        lng = random.uniform(city_bounds['lng_min'], city_bounds['lng_max'])
        
        prefix = random.choice(RESTAURANT_PREFIXES)
        suffix = random.choice(RESTAURANT_SUFFIXES)
        restaurant_name = f"{prefix} {suffix}"
        
        merchants.append({
            'merchant_id': f"MER{str(i+1).zfill(5)}",
            'restaurant_name': restaurant_name,
            'city': city,
            'restaurant_lat': round(lat, 6),
            'restaurant_lng': round(lng, 6),
            'cuisine_type': random.choice(CUISINE_TYPES),
        })
    
    return merchants


def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate the haversine distance between two points in meters."""
    R = 6371000  # Earth's radius in meters
    
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)
    
    a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c


def generate_point_at_distance(lat: float, lng: float, 
                                min_dist_m: float, max_dist_m: float) -> Tuple[float, float]:
    """Generate a random point at a specified distance range from origin."""
    # Random bearing in radians
    bearing = random.uniform(0, 2 * math.pi)
    
    # Random distance within range
    distance = random.uniform(min_dist_m, max_dist_m)
    
    # Earth's radius in meters
    R = 6371000
    
    # Convert to radians
    lat_rad = math.radians(lat)
    lng_rad = math.radians(lng)
    
    # Calculate new position
    new_lat_rad = math.asin(
        math.sin(lat_rad) * math.cos(distance/R) +
        math.cos(lat_rad) * math.sin(distance/R) * math.cos(bearing)
    )
    
    new_lng_rad = lng_rad + math.atan2(
        math.sin(bearing) * math.sin(distance/R) * math.cos(lat_rad),
        math.cos(distance/R) - math.sin(lat_rad) * math.sin(new_lat_rad)
    )
    
    return round(math.degrees(new_lat_rad), 6), round(math.degrees(new_lng_rad), 6)


def generate_random_timestamp(start_date: datetime, end_date: datetime) -> datetime:
    """Generate a random timestamp between start and end dates."""
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)


def is_peak_hour(dt: datetime) -> bool:
    """Check if timestamp falls in peak hours (12pm-2pm or 7pm-10pm)."""
    hour = dt.hour
    return (12 <= hour < 14) or (19 <= hour < 22)


def calculate_actual_kpt(complexity: float, kitchen_load: float, 
                         concurrent_orders: int, is_peak: bool) -> int:
    """
    Calculate actual Kitchen Prep Time in seconds.
    Base KPT is normally distributed around 900-1800 seconds,
    scaled by complexity and kitchen load.
    """
    # Base KPT (mean around 1200 seconds, std dev 300)
    base_kpt = np.random.normal(1200, 300)
    
    # Scale by complexity (0.2 to 1.8)
    complexity_factor = 0.5 + (complexity * 0.5)
    
    # Scale by kitchen load (0.1 to 1.0)
    load_factor = 1 + (kitchen_load * 0.5)
    
    # Concurrent orders impact
    concurrent_factor = 1 + (concurrent_orders - 1) * 0.05
    
    # Peak hour impact
    peak_factor = 1.15 if is_peak else 1.0
    
    kpt = base_kpt * complexity_factor * load_factor * concurrent_factor * peak_factor
    
    # Clamp to reasonable range (480 to 2700 seconds = 8 to 45 minutes)
    kpt = max(480, min(2700, kpt))
    
    return int(kpt)


def generate_label_confidence(is_rider_influenced: bool) -> float:
    """
    Generate label confidence score (0.4 to 1.0).
    Rider-influenced rows have lower confidence.
    """
    if is_rider_influenced:
        # Lower confidence range for rider-influenced
        return round(random.uniform(0.4, 0.7), 2)
    else:
        # Higher confidence for genuine signals
        return round(random.uniform(0.7, 1.0), 2)


# ============================================================================
# MAIN DATA GENERATION
# ============================================================================

def generate_dataset(num_rows: int = NUM_ROWS, 
                     show_progress: bool = True) -> pd.DataFrame:
    """Generate the complete dataset."""
    
    print(f"Generating Zomato-style dataset with {num_rows:,} rows...")
    print("-" * 60)
    
    # Generate merchant pool
    print("Creating merchant pool...")
    merchants = generate_merchant_pool(NUM_MERCHANTS)
    
    # Date range for orders (2023-2024)
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    end_date = datetime(2024, 12, 31, 23, 59, 59)
    
    # Prepare data storage
    data = []
    
    # Progress tracking
    progress_interval = num_rows // 10
    
    print("Generating orders...")
    
    for i in range(num_rows):
        # Progress update
        if show_progress and (i + 1) % progress_interval == 0:
            progress = ((i + 1) / num_rows) * 100
            print(f"  Progress: {progress:.0f}% ({i+1:,} / {num_rows:,} rows)")
        
        # Select random merchant
        merchant = random.choice(merchants)
        
        # Generate unique order ID
        order_id = f"ORD{str(i+1).zfill(8)}"
        
        # ---- TIMESTAMPS ----
        order_confirmed_at = generate_random_timestamp(start_date, end_date)
        
        # Prep time between 8 and 45 minutes
        prep_time_minutes = random.randint(8, 45)
        for_timestamp = order_confirmed_at + timedelta(minutes=prep_time_minutes)
        
        # Rider arrival: typically around FOR time (+/- few minutes)
        rider_arrival_offset = random.randint(-5, 10)  # minutes relative to FOR
        rider_arrived_at = for_timestamp + timedelta(minutes=rider_arrival_offset)
        
        # ---- RIDER INFO ----
        rider_id = f"RDR{random.randint(1, 10000):05d}"
        
        # 35% chance rider is nearby (rider-influenced FOR)
        is_for_rider_influenced = random.random() < 0.35
        
        if is_for_rider_influenced:
            # Rider within 200 meters of restaurant
            rider_lat, rider_lng = generate_point_at_distance(
                merchant['restaurant_lat'], 
                merchant['restaurant_lng'],
                0, 200
            )
        else:
            # Rider 500m to 3km away
            rider_lat, rider_lng = generate_point_at_distance(
                merchant['restaurant_lat'], 
                merchant['restaurant_lng'],
                500, 3000
            )
        
        # ---- ORDER ITEMS ----
        num_items = random.randint(1, 6)
        item_category = random.choice(ITEM_CATEGORIES)
        
        # Complexity score based on category
        complexity_range = ITEM_COMPLEXITY[item_category]
        item_complexity_score = round(random.uniform(*complexity_range), 2)
        
        # ---- KITCHEN LOAD ----
        concurrent_orders = random.randint(1, 8)
        kitchen_load_index = round(random.uniform(0.1, 1.0), 2)
        is_peak = is_peak_hour(order_confirmed_at)
        day_of_week = DAYS_OF_WEEK[order_confirmed_at.weekday()]
        is_weekend = order_confirmed_at.weekday() >= 5
        weather = random.choices(WEATHER_CONDITIONS, weights=WEATHER_WEIGHTS, k=1)[0]
        
        # ---- LABELS ----
        actual_kpt_seconds = calculate_actual_kpt(
            item_complexity_score, 
            kitchen_load_index, 
            concurrent_orders, 
            is_peak
        )
        label_confidence = generate_label_confidence(is_for_rider_influenced)
        
        # Compile row
        row = {
            # Order & Merchant Info
            'order_id': order_id,
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
        }
        
        data.append(row)
    
    # Convert to DataFrame
    print("\nConverting to DataFrame...")
    df = pd.DataFrame(data)
    
    print(f"Dataset generation complete!")
    print("-" * 60)
    
    return df


def save_dataset(df: pd.DataFrame, output_path: str = 'zomato_orders_100k.csv'):
    """Save the dataset to CSV."""
    print(f"Saving dataset to {output_path}...")
    df.to_csv(output_path, index=False)
    print(f"Saved successfully! File size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")


def print_summary(df: pd.DataFrame):
    """Print dataset summary statistics."""
    print("\n" + "=" * 60)
    print("DATASET SUMMARY")
    print("=" * 60)
    
    print(f"\nTotal rows: {len(df):,}")
    print(f"Total columns: {len(df.columns)}")
    
    print(f"\n--- City Distribution ---")
    print(df['city'].value_counts().to_string())
    
    print(f"\n--- Cuisine Type Distribution ---")
    print(df['cuisine_type'].value_counts().to_string())
    
    print(f"\n--- FOR Rider Influenced ---")
    influenced_count = df['is_for_rider_influenced'].sum()
    print(f"  Rider-influenced (within 200m): {influenced_count:,} ({influenced_count/len(df)*100:.1f}%)")
    print(f"  Genuine signals (500m-3km): {len(df) - influenced_count:,} ({(len(df) - influenced_count)/len(df)*100:.1f}%)")
    
    print(f"\n--- KPT Statistics ---")
    print(f"  Mean: {df['actual_kpt_seconds'].mean():.1f} seconds ({df['actual_kpt_seconds'].mean()/60:.1f} minutes)")
    print(f"  Std Dev: {df['actual_kpt_seconds'].std():.1f} seconds")
    print(f"  Min: {df['actual_kpt_seconds'].min()} seconds")
    print(f"  Max: {df['actual_kpt_seconds'].max()} seconds")
    
    print(f"\n--- Peak Hour Orders ---")
    peak_count = df['is_peak_hour'].sum()
    print(f"  Peak hours: {peak_count:,} ({peak_count/len(df)*100:.1f}%)")
    print(f"  Off-peak: {len(df) - peak_count:,} ({(len(df) - peak_count)/len(df)*100:.1f}%)")
    
    print(f"\n--- Sample Data (first 5 rows) ---")
    print(df.head().to_string())
    
    print("\n" + "=" * 60)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description='Generate Zomato-style food delivery dataset')
    parser.add_argument('--rows', type=int, default=NUM_ROWS, 
                        help=f'Number of rows to generate (default: {NUM_ROWS:,})')
    parser.add_argument('--output', type=str, default='zomato_orders_100k.csv',
                        help='Output CSV file path')
    parser.add_argument('--seed', type=int, default=RANDOM_SEED,
                        help=f'Random seed for reproducibility (default: {RANDOM_SEED})')
    parser.add_argument('--no-summary', action='store_true',
                        help='Skip printing summary statistics')
    
    args = parser.parse_args()
    
    # Set seed if different from default
    if args.seed != RANDOM_SEED:
        random.seed(args.seed)
        np.random.seed(args.seed)
        Faker.seed(args.seed)
    
    # Generate dataset
    df = generate_dataset(num_rows=args.rows)
    
    # Print summary
    if not args.no_summary:
        print_summary(df)
    
    # Save to CSV
    output_path = os.path.join(os.path.dirname(__file__), args.output)
    save_dataset(df, output_path)
    
    print(f"\nDone! Dataset saved to: {output_path}")
