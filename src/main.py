import argparse
import sys
from typing import Dict, Any
from dotenv import load_dotenv
from graph.workflow import AgentWorkflow
from util.config import ConfigParser
from util.logger import logger
from util.cs2_db_helper import cs2_db_initialize, get_cs2_db

# Load environment variables from .env file
load_dotenv()

def load_portfolio_config(cfg: Dict[str, Any], db):
    """Load and validate config based on experiment configuration."""
    config_id = db.get_config_id_by_name(cfg["exp_name"])
    if not config_id:
        logger.info(f"Creating new config for {cfg['exp_name']}")
        config_id = db.create_config(cfg)
        if not config_id:
            raise RuntimeError(f"Failed to create config for {cfg['exp_name']}")
    return config_id

def main():
    """Main entry point for the CS2 MARKET System."""
    parser = argparse.ArgumentParser(description='BUFFERFLY: Pilot for Your Next CS2 MARKET Investment')
    parser.add_argument('--config', type=str, required=True, help='Path to configuration YAML file')
    parser.add_argument('--trading-date', type=str, required=True, help='Trading date in YYYY-MM-DD format')
    parser.add_argument('--local-db', action='store_true', help='Use local SQLite database instead of Supabase')
    
    args = parser.parse_args()
    
    # Load configuration
    cfg = ConfigParser(args).get_config()
    cs2_db_initialize(use_local_db=args.local_db)
    cs2_db = get_cs2_db()
        
    logger.info(f"📊 Loading configuration: {cfg['exp_name']}")
    logger.info(f"📅 Trading date: {args.trading_date}")
        
    # Load portfolio config
    config_id = load_portfolio_config(cfg, cs2_db)
        
     # Check trading date order
    latest_trading_date = cs2_db.get_latest_trading_date(config_id)
        
    if latest_trading_date and latest_trading_date > cfg["trading_date"]:
        raise RuntimeError(f"Trading date {args.trading_date} is not in chronological order")

    try:    
        # Run workflow
        app = AgentWorkflow(cfg, config_id)
        time_cost = app.run(config_id)
        logger.info(f"✅ Analysis completed! Time cost: {time_cost:.2f} seconds")
    except Exception as e:
        logger.error(f"❌ Error during analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
