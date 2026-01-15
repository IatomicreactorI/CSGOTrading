import os
import logging
from datetime import datetime
from graph.schema import Decision, AnalystSignal, Portfolio, PositionRisk

class CS2Logger:
    """Logger for the CS2 application."""

    def __init__(self, log_level: str = 'INFO'):
        """Initialize the logger.
        
        Args:
            log_dict: Dictionary containing log configuration.
        """
        self.log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))
        self.log_level = log_level
        
        # Create log directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger("cs2")
        self.logger.setLevel(self.log_level)
        # Disable propagation to prevent duplicate log messages from parent loggers
        self.logger.propagate = False
        
        # Clear existing handlers to prevent duplicates on module reload
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Create file handler
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.log_dir, f"cs2_{timestamp}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(self.log_level)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        
        # Create formatter
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def debug(self, message: str):
        """Log a debug message."""
        self.logger.debug(message)

    def info(self, message: str):
        """Log an info message."""
        self.logger.info(message)

    def warning(self, message: str):
        """Log a warning message."""
        self.logger.warning(message)

    def error(self, message: str):
        """Log an error message."""
        self.logger.error(message)

    def log_agent_status(self, agent_name: str, ticker: str, status: str):
        """Log the status of an agent."""
        if ticker:
            msg = f"Agent: {agent_name} | Ticker: {ticker} | Status: {status}"
        else:
            msg = f"Agent: {agent_name} | Status: {status}"

        self.info(msg)

    def log_decision(self, ticker: str, d: Decision):
        """Log the decision of a ticker."""
        msg = f"Decision for {ticker}: {d.action} | Shares: {d.shares} | Price: {d.price} | Justification: {d.justification}"
        self.info(msg)

    def log_signal(self, agent_name: str, ticker: str, s: AnalystSignal):
        """Log the signal of a ticker."""
        msg = f"Agent: {agent_name} | Ticker: {ticker} | Signal: {s.signal} | Justification: {s.justification}"
        self.info(msg)

    def log_portfolio(self, msg: str, portfolio: Portfolio):
        """Log the portfolio."""
        asset_value = portfolio.cashflow + sum(position.value for position in portfolio.positions.values())
        self.info(f"{msg}: {portfolio} | Total Asset Value: {asset_value:.2f}")

    def log_risk(self, ticker: str, position_risk: PositionRisk):
        """Log the risk assessment of a ticker."""
        msg = f"Risk Control for {ticker}| Optimal Position Ratio: {position_risk.optimal_position_ratio} | Justification: {position_risk.justification}"
        self.info(msg)
        
# Create a global logger instance
logger = CS2Logger()
