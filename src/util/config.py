import yaml
from datetime import datetime
from util.logger import logger
from typing import Dict, Any

class ConfigParser:
    """Manages configuration loading and validation."""

    def __init__(self, args):
        """Initialize the configuration manager."""
        self.config_path = args.config
        self.trading_date = args.trading_date
        self.config = self.load_config()
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        cfg = {}
        try:
            with open(self.config_path, 'r') as f:
                logger.info(f"Loading configuration from {self.config_path}")
                cfg = yaml.safe_load(f)
        except FileNotFoundError:
            raise ValueError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file: {e}")
        
        cfg['trading_date'] = datetime.strptime(self.trading_date, '%Y-%m-%d')
        cfg['planner_mode'] = cfg.get('planner_mode', False)

        return cfg

    def get_config(self) -> Dict[str, Any]:
        """Get the configuration."""
        return self.config
