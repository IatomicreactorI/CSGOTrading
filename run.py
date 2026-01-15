"""
Batch run CS2 experiments script
Run experiments from specified start date to end date
Support specifying experiment configuration through command line arguments
Automatically discover available experiments from the configuration file directory
"""

import sys
import os
import argparse
import yaml
from typing import Dict, Any
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from graph.workflow import AgentWorkflow
from util.config import ConfigParser
from util.logger import logger
from util.cs2_db_helper import cs2_db_initialize, get_cs2_db

_script_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(_script_dir)


def load_portfolio_config(cfg: Dict[str, Any], db):
    """Load and validate config based on experiment configuration."""
    config_id = db.get_config_id_by_name(cfg["exp_name"])
    if not config_id:
        logger.info(f"Creating new config for {cfg['exp_name']}")
        config_id = db.create_config(cfg)
        if not config_id:
            raise RuntimeError(f"Failed to create config for {cfg['exp_name']}")
    return config_id


def run_single_experiment(config_path: str, trading_date: str, use_local_db: bool = True):
    """
    Run a single trading date experiment.
    """
    # Create a mock args object for ConfigParser
    class Args:
        def __init__(self, config_path, trading_date, use_local_db):
            self.config = config_path
            self.trading_date = trading_date
            self.local_db = use_local_db
    
    args = Args(config_path, trading_date, use_local_db)
    
    # Load configuration
    cfg = ConfigParser(args).get_config()
    cs2_db_initialize(use_local_db=use_local_db)
    cs2_db = get_cs2_db()
        
    logger.info(f"Loading configuration: {cfg['exp_name']}")
    logger.info(f"Trading date: {trading_date}")
        
    # Load portfolio config
    config_id = load_portfolio_config(cfg, cs2_db)
        
    # Check trading date order
    latest_trading_date = cs2_db.get_latest_trading_date(config_id)
        
    if latest_trading_date and latest_trading_date > cfg["trading_date"]:
        raise RuntimeError(f"Trading date {trading_date} is not in chronological order")

    # Run workflow
    app = AgentWorkflow(cfg, config_id)
    time_cost = app.run(config_id)
    logger.info(f"Analysis completed! Time cost: {time_cost:.2f} seconds")
    
    return time_cost


def run_experiment(trading_date: str, config_path: str, use_local_db: bool = True):
    """Run single date experiment with error handling"""
    
    print(f"\n{'='*80}")
    print(f"Running: {trading_date}")
    print(f"{'='*80}\n")
    
    try:
        run_single_experiment(config_path, trading_date, use_local_db)
        print(f"\n{trading_date} run successfully")
        return True
    except Exception as e:
        logger.error(f"Error running experiment for {trading_date}: {e}")
        print(f"\n{trading_date} run failed: {e}")
        return False


def main():
    """Main function: batch run experiments"""
    # Load environment variables
    load_dotenv()
    
    epilog = """
Example usage:
  
  # Specify date range
  python run.py --config T-ds.yaml --start-date 2025-09-25 --end-date 2025-11-15
    """
    
    parser = argparse.ArgumentParser(
        description='Batch run CS2 experiments',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog
    )
    
    parser.add_argument('--config', type=str, required=True, help='Configuration file name (automatically search in src/config/ directory, e.g. T-ds.yaml)')
    parser.add_argument('--start-date', type=str, default='2025-09-25', help='Start date (format: YYYY-MM-DD, default: 2025-09-25)')
    parser.add_argument('--end-date', type=str, default='2025-10-27', help='End date (format: YYYY-MM-DD, default: 2025-10-27)')
    parser.add_argument('--no-local-db', action='store_true', help='Do not use local database (default: use local database)')
    
    args = parser.parse_args()
    
    config_path = args.config
    
    # Only support file name, not contain path
    if os.path.dirname(config_path):
        print(f"Error: config file path can only use file name, not contain path")
        print(f"Correct usage: python run.py --config T-ds.yaml")
        sys.exit(1)
    
    try:
        abs_config_path = os.path.join(PROJECT_ROOT, "src", "config", config_path)
            
        with open(abs_config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            exp_name = config.get('exp_name')
        config_path = os.path.join("config", config_path)
    except FileNotFoundError as e:
        print(f"Error: config file '{config_path}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: failed to read config file '{config_path}': {e}")
        sys.exit(1)
    
    # Parse date
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        print(f"Error: date format is incorrect - {e}")
        print("Date format should be: YYYY-MM-DD (e.g. 2025-09-25)")
        sys.exit(1)
    
    if start_date > end_date:
        print("Error: start date cannot be later than end date")
        sys.exit(1)
    
    use_local_db = not args.no_local_db
    
    # Ensure log directory exists
    Path(PROJECT_ROOT, "src", "logs").mkdir(parents=True, exist_ok=True)
    
    current_date = start_date
    
    print(f"\n{'='*80}")
    print(f"Start batch running experiments")
    print(f"Configuration file: {config_path}")
    print(f"Experiment name: {exp_name}")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Total days: {(end_date - start_date).days + 1}")
    print(f"Use local database: {use_local_db}")
    print(f"{'='*80}\n")
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        success = run_experiment(date_str, config_path, use_local_db=use_local_db)
        
        if not success:
            print(f"\nError: {date_str} run failed, program exit")
            sys.exit(1)
        
        current_date += timedelta(days=1)
    
    # Output summary
    print(f"\n{'='*80}")
    print(f"Batch running completed")
    print(f"Successfully run {((end_date - start_date).days + 1)} days")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()

