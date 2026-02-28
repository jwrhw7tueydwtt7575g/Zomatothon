def to_serializable(val):
    """Recursively convert numpy types to native Python types for JSON serialization."""
    import numpy as np
    if isinstance(val, dict):
        return {to_serializable(k): to_serializable(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [to_serializable(x) for x in val]
    elif isinstance(val, tuple):
        return tuple(to_serializable(x) for x in val)
    elif isinstance(val, (np.integer, np.int32, np.int64)):
        return int(val)
    elif isinstance(val, (np.floating, np.float32, np.float64)):
        return float(val)
    else:
        return val
#!/usr/bin/env python3
"""
Standalone Model Training Script for Zomato KPT Prediction
- Pulls data from S3 s3://zomatoovertomato/raw-orders/
- Runs signal audit + label decontamination
- Trains XGBoost model with MLflow tracking
- Saves model artifacts to s3://zomatoovertomato/models/
"""

import os
import sys
import math
import json
import logging
import argparse
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import boto3
import numpy as np
import pandas as pd
import mlflow
import mlflow.xgboost
from botocore.exceptions import ClientError
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
import xgboost as xgb

# ============================================================================
# CONFIGURATION
# ============================================================================

S3_BUCKET = os.getenv('S3_BUCKET', 'zomatoovertomato')
S3_DATA_PREFIX = os.getenv('S3_DATA_PREFIX', 'raw-orders')
S3_MODEL_PREFIX = os.getenv('S3_MODEL_PREFIX', 'models')
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')

MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000')
MLFLOW_EXPERIMENT_NAME = os.getenv('MLFLOW_EXPERIMENT_NAME', 'zomato-kpt-prediction')

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('train_model.log')
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# HAVERSINE DISTANCE (for signal audit)
# ============================================================================

def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate haversine distance between two points in meters."""
    R = 6371000  # Earth's radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)
    
    a = (math.sin(delta_phi/2)**2 + 
         math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c


# ============================================================================
# S3 DATA LOADER
# ============================================================================

class S3DataLoader:
    """Loads data from S3 bucket."""
    
    def __init__(self, bucket: str, prefix: str, region: str):
        self.bucket = bucket
        self.prefix = prefix
        self.s3_client = boto3.client('s3', region_name=region)
        logger.info(f"S3 Data Loader initialized for s3://{bucket}/{prefix}")
    
    def list_parquet_files(self) -> List[str]:
        """List all Parquet files in the prefix."""
        files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    files.append(obj['Key'])
        
        logger.info(f"Found {len(files)} Parquet files in s3://{self.bucket}/{self.prefix}")
        return files
    
    def load_parquet_files(self, max_files: Optional[int] = None) -> pd.DataFrame:
        """Load Parquet files from S3 and concatenate."""
        files = self.list_parquet_files()
        
        if max_files:
            files = files[:max_files]
            logger.info(f"Loading first {max_files} files")
        
        if not files:
            logger.warning("No Parquet files found!")
            return pd.DataFrame()
        
        dfs = []
        for s3_key in files:
            try:
                # Download to temp file
                with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
                    self.s3_client.download_file(self.bucket, s3_key, tmp.name)
                    df = pd.read_parquet(tmp.name)
                    dfs.append(df)
            except Exception as e:
                logger.error(f"Error loading {s3_key}: {e}")
        
        if not dfs:
            return pd.DataFrame()
        
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Loaded {len(combined_df)} total records from {len(dfs)} files")
        return combined_df
    
    def load_from_local_backup(self, backup_dir: str) -> pd.DataFrame:
        """Load from local CSV backup (fallback)."""
        backup_path = Path(backup_dir)
        if not backup_path.exists():
            logger.warning(f"Backup directory not found: {backup_dir}")
            return pd.DataFrame()
        
        csv_files = list(backup_path.glob('*.csv'))
        if not csv_files:
            logger.warning("No CSV backup files found")
            return pd.DataFrame()
        
        dfs = [pd.read_csv(f) for f in csv_files]
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Loaded {len(combined_df)} records from local backup")
        return combined_df


# ============================================================================
# SIGNAL AUDIT
# ============================================================================

class SignalAuditor:
    """Performs signal audit to detect rider-influenced labels."""
    
    GPS_PROXIMITY_THRESHOLD = 200  # meters
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.audit_results = {}
    
    def calculate_rider_distance(self) -> pd.Series:
        """Calculate distance between rider and restaurant."""
        distances = []
        for _, row in self.df.iterrows():
            dist = haversine_distance(
                row['restaurant_lat'], row['restaurant_lng'],
                row['rider_lat'], row['rider_lng']
            )
            distances.append(dist)
        return pd.Series(distances, index=self.df.index)
    
    def run_audit(self) -> pd.DataFrame:
        """Run full signal audit."""
        logger.info("Running signal audit...")
        
        # Calculate rider distance
        self.df['rider_distance_m'] = self.calculate_rider_distance()
        
        # GPS-based dirty label detection
        self.df['gps_detected_dirty'] = self.df['rider_distance_m'] < self.GPS_PROXIMITY_THRESHOLD
        
        # Compare with reported rider influence
        self.df['signal_match'] = self.df['gps_detected_dirty'] == self.df['is_for_rider_influenced']
        
        # Audit statistics
        total = len(self.df)
        dirty_reported = self.df['is_for_rider_influenced'].sum()
        dirty_detected = self.df['gps_detected_dirty'].sum()
        signal_match = self.df['signal_match'].sum()
        
        self.audit_results = {
            'total_records': total,
            'dirty_labels_reported': int(dirty_reported),
            'dirty_labels_detected': int(dirty_detected),
            'signal_match_rate': round(signal_match / total * 100, 2),
            'contamination_rate': round(dirty_detected / total * 100, 2)
        }
        
        logger.info(f"Signal Audit Results: {json.dumps(self.audit_results, indent=2)}")
        return self.df
    
    def get_audit_results(self) -> Dict:
        return self.audit_results


# ============================================================================
# LABEL DECONTAMINATOR
# ============================================================================

class LabelDecontaminator:
    """Applies label decontamination strategies."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
    
    def strategy_exclude(self) -> pd.DataFrame:
        """Strategy 1: Exclude all dirty labels."""
        clean_df = self.df[~self.df['gps_detected_dirty']].copy()
        logger.info(f"Exclude strategy: {len(self.df)} -> {len(clean_df)} records")
        return clean_df
    
    def strategy_weighted(self) -> pd.DataFrame:
        """Strategy 2: Keep all but add sample weights."""
        weighted_df = self.df.copy()
        # Lower weight for dirty labels
        weighted_df['sample_weight'] = np.where(
            weighted_df['gps_detected_dirty'],
            0.3,  # Dirty labels get lower weight
            1.0   # Clean labels get full weight
        )
        logger.info(f"Weighted strategy: Added sample weights, dirty weight=0.3")
        return weighted_df
    
    def strategy_confidence(self) -> pd.DataFrame:
        """Strategy 3: Use label confidence as weights."""
        conf_df = self.df.copy()
        conf_df['sample_weight'] = conf_df['label_confidence']
        logger.info(f"Confidence strategy: Using label_confidence as weights")
        return conf_df


# ============================================================================
# FEATURE ENGINEER
# ============================================================================

class FeatureEngineer:
    """Creates features for model training."""
    
    CATEGORICAL_COLS = ['city', 'cuisine_type', 'item_category', 'day_of_week', 'weather_condition']
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
    
    def create_features(self) -> pd.DataFrame:
        """Create all features for model."""
        logger.info("Creating features...")
        
        # Parse timestamps
        for col in ['order_confirmed_at', 'for_timestamp', 'rider_arrived_at']:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        
        # Time-based features
        if 'order_confirmed_at' in self.df.columns:
            self.df['hour_of_day'] = self.df['order_confirmed_at'].dt.hour
            self.df['minute_of_hour'] = self.df['order_confirmed_at'].dt.minute
            self.df['is_lunch_hour'] = self.df['hour_of_day'].between(12, 14).astype(int)
            self.df['is_dinner_hour'] = self.df['hour_of_day'].between(19, 22).astype(int)
        
        # Distance feature (if not already calculated)
        if 'rider_distance_m' not in self.df.columns:
            distances = []
            for _, row in self.df.iterrows():
                dist = haversine_distance(
                    row['restaurant_lat'], row['restaurant_lng'],
                    row['rider_lat'], row['rider_lng']
                )
                distances.append(dist)
            self.df['rider_distance_m'] = distances
        
        # Kitchen load composite
        self.df['kitchen_load_composite'] = (
            self.df['kitchen_load_index'] * 0.5 +
            self.df['concurrent_orders'] / 8 * 0.3 +
            self.df['is_peak_hour'].astype(int) * 0.2
        )
        
        # Item value score
        self.df['item_value_score'] = self.df['num_items'] * self.df['item_complexity_score']
        
        # Encode categoricals
        for col in self.CATEGORICAL_COLS:
            if col in self.df.columns:
                self.df[f'{col}_encoded'] = self.df[col].astype('category').cat.codes
        
        # Boolean to int
        bool_cols = ['is_peak_hour', 'is_weekend']
        for col in bool_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(int)
        
        logger.info(f"Feature engineering complete. Shape: {self.df.shape}")
        return self.df
    
    def get_feature_columns(self) -> List[str]:
        """Get list of feature columns for modeling."""
        feature_cols = [
            # Numerical features
            'restaurant_lat', 'restaurant_lng',
            'rider_lat', 'rider_lng', 'rider_distance_m',
            'num_items', 'item_complexity_score',
            'concurrent_orders', 'kitchen_load_index',
            'is_peak_hour', 'is_weekend',
            'hour_of_day', 'minute_of_hour',
            'is_lunch_hour', 'is_dinner_hour',
            'kitchen_load_composite', 'item_value_score',
            
            # Encoded categoricals
            'city_encoded', 'cuisine_type_encoded',
            'item_category_encoded', 'day_of_week_encoded',
            'weather_condition_encoded'
        ]
        
        # Return only columns that exist
        return [c for c in feature_cols if c in self.df.columns]


# ============================================================================
# MODEL TRAINER
# ============================================================================

class ModelTrainer:
    """Trains XGBoost model with MLflow tracking."""
    
    def __init__(self, mlflow_uri: str, experiment_name: str,
                 s3_bucket: str, model_prefix: str, region: str):
        self.s3_bucket = s3_bucket
        self.model_prefix = model_prefix
        self.s3_client = boto3.client('s3', region_name=region)
        
        # Setup MLflow
        mlflow.set_tracking_uri(mlflow_uri)
        mlflow.set_experiment(experiment_name)
        logger.info(f"MLflow tracking URI: {mlflow_uri}")
        logger.info(f"MLflow experiment: {experiment_name}")
    
    def train_model(self, df: pd.DataFrame, feature_cols: List[str],
                    target_col: str = 'actual_kpt_seconds',
                    weight_col: Optional[str] = None,
                    model_name: str = 'xgb_kpt_model') -> Dict:
        """Train XGBoost model and log to MLflow."""
        
        logger.info(f"Training model: {model_name}")
        logger.info(f"Features: {len(feature_cols)}, Target: {target_col}")
        
        # Prepare data
        X = df[feature_cols].copy()
        y = df[target_col].copy()
        
        # Handle weights
        weights = df[weight_col].values if weight_col and weight_col in df.columns else None
        
        # Train/test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        if weights is not None:
            w_train, w_test = train_test_split(weights, test_size=0.2, random_state=42)
        else:
            w_train, w_test = None, None
        
        # XGBoost parameters
        params = {
            'objective': 'reg:squarederror',
            'max_depth': 8,
            'learning_rate': 0.1,
            'n_estimators': 200,
            'min_child_weight': 3,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': 42,
            'n_jobs': -1
        }
        
        # Start MLflow run
        with mlflow.start_run(run_name=model_name) as run:
            run_id = run.info.run_id
            logger.info(f"MLflow Run ID: {run_id}")
            
            # Log parameters
            mlflow.log_params(params)
            mlflow.log_param('num_features', len(feature_cols))
            mlflow.log_param('training_samples', len(X_train))
            mlflow.log_param('test_samples', len(X_test))
            mlflow.log_param('weight_col', weight_col or 'None')
            
            # Train model
            model = xgb.XGBRegressor(**params)
            model.fit(
                X_train, y_train,
                sample_weight=w_train,
                eval_set=[(X_test, y_test)],
                verbose=False
            )
            
            # Predictions
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)
            
            # Calculate metrics
            train_mae = mean_absolute_error(y_train, y_pred_train)
            test_mae = mean_absolute_error(y_test, y_pred_test)
            train_rmse = np.sqrt(mean_squared_error(y_train, y_pred_train))
            test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
            
            # Percentile metrics
            test_errors = np.abs(y_test.values - y_pred_test)
            p50 = np.percentile(test_errors, 50)
            p90 = np.percentile(test_errors, 90)
            p95 = np.percentile(test_errors, 95)
            
            # Log metrics
            metrics = {
                'train_mae': train_mae,
                'test_mae': test_mae,
                'train_rmse': train_rmse,
                'test_rmse': test_rmse,
                'test_p50_error': p50,
                'test_p90_error': p90,
                'test_p95_error': p95
            }
            mlflow.log_metrics(metrics)
            
            logger.info(f"Metrics: MAE={test_mae:.2f}, RMSE={test_rmse:.2f}, P90={p90:.2f}")
            
            # Feature importance
            # Convert feature importances to float to avoid numpy.float32 serialization issues
            importance = dict(zip([str(k) for k in feature_cols], [float(v) for v in model.feature_importances_.tolist()]))
            importance_sorted = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))
            # Log feature importance as artifact
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(to_serializable(importance_sorted), f, indent=2)
                mlflow.log_artifact(f.name, 'feature_importance')
            
            # Log model
            mlflow.xgboost.log_model(model, 'model')
            
            # Save model to S3
            model_s3_key = self._save_model_to_s3(model, model_name, run_id)
            mlflow.log_param('s3_model_path', f"s3://{self.s3_bucket}/{model_s3_key}")
            
            return {
                'run_id': run_id,
                'model_name': model_name,
                'metrics': metrics,
                's3_path': f"s3://{self.s3_bucket}/{model_s3_key}",
                'feature_importance_top5': dict(list(importance_sorted.items())[:5])
            }
    
    def _save_model_to_s3(self, model: xgb.XGBRegressor, model_name: str, run_id: str) -> str:
        """Save model to S3."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{model_name}_{timestamp}_{run_id[:8]}.json"
        s3_key = f"{self.model_prefix}/{filename}"
        
        # Save to temp file
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
            model.save_model(tmp.name)
            self.s3_client.upload_file(tmp.name, self.s3_bucket, s3_key)
        
        logger.info(f"Model saved to s3://{self.s3_bucket}/{s3_key}")
        return s3_key


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_training_pipeline(
    s3_bucket: str,
    s3_data_prefix: str,
    s3_model_prefix: str,
    aws_region: str,
    mlflow_uri: str,
    experiment_name: str,
    max_files: Optional[int] = None,
    local_backup_dir: Optional[str] = None,
    decontamination_strategy: str = 'weighted'
) -> Dict:
    """Run the full training pipeline."""
    
    logger.info("=" * 60)
    logger.info("Starting Model Training Pipeline")
    logger.info("=" * 60)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'status': 'started'
    }
    
    try:
        # Step 1: Load data from S3
        logger.info("Step 1: Loading data from S3...")
        loader = S3DataLoader(s3_bucket, s3_data_prefix, aws_region)
        df = loader.load_parquet_files(max_files)
        
        # Fallback to local backup if S3 is empty
        if df.empty and local_backup_dir:
            logger.info("S3 empty, loading from local backup...")
            df = loader.load_from_local_backup(local_backup_dir)
        
        if df.empty:
            raise ValueError("No data available for training!")
        
        results['data_records'] = len(df)
        logger.info(f"Loaded {len(df)} records")
        
        # Step 2: Signal Audit
        logger.info("Step 2: Running Signal Audit...")
        auditor = SignalAuditor(df)
        df = auditor.run_audit()
        results['signal_audit'] = auditor.get_audit_results()
        
        # Step 3: Label Decontamination
        logger.info(f"Step 3: Label Decontamination (strategy: {decontamination_strategy})...")
        decontaminator = LabelDecontaminator(df)
        
        if decontamination_strategy == 'exclude':
            df = decontaminator.strategy_exclude()
            weight_col = None
        elif decontamination_strategy == 'weighted':
            df = decontaminator.strategy_weighted()
            weight_col = 'sample_weight'
        elif decontamination_strategy == 'confidence':
            df = decontaminator.strategy_confidence()
            weight_col = 'sample_weight'
        else:
            weight_col = None
        
        results['decontamination_strategy'] = decontamination_strategy
        results['records_after_decontamination'] = len(df)
        
        # Step 4: Feature Engineering
        logger.info("Step 4: Feature Engineering...")
        engineer = FeatureEngineer(df)
        df = engineer.create_features()
        feature_cols = engineer.get_feature_columns()
        results['num_features'] = len(feature_cols)
        
        # Step 5: Model Training
        logger.info("Step 5: Training Model...")
        trainer = ModelTrainer(
            mlflow_uri=mlflow_uri,
            experiment_name=experiment_name,
            s3_bucket=s3_bucket,
            model_prefix=s3_model_prefix,
            region=aws_region
        )
        
        model_results = trainer.train_model(
            df=df,
            feature_cols=feature_cols,
            target_col='actual_kpt_seconds',
            weight_col=weight_col,
            model_name=f'xgb_kpt_{decontamination_strategy}'
        )
        
        results['model'] = model_results
        results['status'] = 'success'
        
        logger.info("=" * 60)
        logger.info("Pipeline Complete!")
        logger.info(f"MLflow Run ID: {model_results['run_id']}")
        logger.info(f"Model S3 Path: {model_results['s3_path']}")
        logger.info(f"Test MAE: {model_results['metrics']['test_mae']:.2f} seconds")
        logger.info("=" * 60)
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        results['status'] = 'failed'
        results['error'] = str(e)
        raise
    
    return results


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Train KPT Prediction Model')
    parser.add_argument('--s3-bucket', default=S3_BUCKET, help='S3 bucket name')
    parser.add_argument('--s3-data-prefix', default=S3_DATA_PREFIX, help='S3 data prefix')
    parser.add_argument('--s3-model-prefix', default=S3_MODEL_PREFIX, help='S3 model prefix')
    parser.add_argument('--aws-region', default=AWS_REGION, help='AWS region')
    parser.add_argument('--mlflow-uri', default=MLFLOW_TRACKING_URI, help='MLflow tracking URI')
    parser.add_argument('--experiment', default=MLFLOW_EXPERIMENT_NAME, help='MLflow experiment name')
    parser.add_argument('--max-files', type=int, default=None, help='Max S3 files to load')
    parser.add_argument('--local-backup', default='./data/backup', help='Local backup directory')
    parser.add_argument('--strategy', default='weighted',
                        choices=['exclude', 'weighted', 'confidence'],
                        help='Decontamination strategy')
    
    args = parser.parse_args()
    
    results = run_training_pipeline(
        s3_bucket=args.s3_bucket,
        s3_data_prefix=args.s3_data_prefix,
        s3_model_prefix=args.s3_model_prefix,
        aws_region=args.aws_region,
        mlflow_uri=args.mlflow_uri,
        experiment_name=args.experiment,
        max_files=args.max_files,
        local_backup_dir=args.local_backup,
        decontamination_strategy=args.strategy
    )
    
    # Print results as JSON
    print("\n" + "=" * 60)
    print("PIPELINE RESULTS:")
    print("=" * 60)
    print(json.dumps(results, indent=2, default=str))


if __name__ == '__main__':
    main()
