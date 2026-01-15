import os
import sqlite3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

CS2_DB_PATH = os.getenv("CS2_DB_PATH")
os.makedirs(os.path.dirname(CS2_DB_PATH), exist_ok=True)

def init_cs2_database():
    """Initialize the CS2-specific SQLite database and create tables if they don't exist."""
    conn = sqlite3.connect(CS2_DB_PATH)
    cursor = conn.cursor()

    # Create CS2 config table 
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cs2_config (
        id VARCHAR(36) PRIMARY KEY,
        exp_name VARCHAR(100) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        items JSON NOT NULL,  -- CS2 items list
        has_planner BOOLEAN NOT NULL DEFAULT FALSE,
        llm_model VARCHAR(50) NOT NULL,
        llm_provider VARCHAR(50) NOT NULL,
        market_type VARCHAR(20) DEFAULT 'cs2'  -- mark as CS2 market
    )
    ''')

    # Create CS2 portfolio table 
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cs2_portfolio (
        id VARCHAR(36) PRIMARY KEY,
        config_id VARCHAR(36) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        trading_date TIMESTAMP NOT NULL,                   
        cashflow DECIMAL(15,2) NOT NULL,
        total_assets DECIMAL(15,2) NOT NULL,
        positions JSON NOT NULL,  -- CS2 item positions
        market_type VARCHAR(20) DEFAULT 'cs2',
        FOREIGN KEY (config_id) REFERENCES cs2_config(id)
    )
    ''')

    # Create CS2 decision table 
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cs2_decision (
        id VARCHAR(36) PRIMARY KEY,
        portfolio_id VARCHAR(36) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        trading_date TIMESTAMP NOT NULL,
        item_name VARCHAR(100) NOT NULL,  
        llm_prompt TEXT NOT NULL,
        action VARCHAR(10) NOT NULL,  -- buy, sell, hold
        quantity INTEGER NOT NULL,  -- item quantity
        price DECIMAL(15,2) NOT NULL,
        justification TEXT NOT NULL,
        market_type VARCHAR(20) DEFAULT 'cs2',
        FOREIGN KEY (portfolio_id) REFERENCES cs2_portfolio(id)
    )
    ''')

    # Create CS2 signal table 
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cs2_signal (
        id VARCHAR(36) PRIMARY KEY,
        portfolio_id VARCHAR(36) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        item_name VARCHAR(100) NOT NULL,  -- CS2 item name
        llm_prompt TEXT NOT NULL,
        analyst VARCHAR(50) NOT NULL,
        signal VARCHAR(10) NOT NULL,  -- bullish, bearish, neutral
        justification TEXT NOT NULL,
        market_type VARCHAR(20) DEFAULT 'cs2',
        FOREIGN KEY (portfolio_id) REFERENCES cs2_portfolio(id)
    )
    ''')


    # Create indices for CS2 tables
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_config_exp_name ON cs2_config(exp_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_config_market_type ON cs2_config(market_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_portfolio_updated ON cs2_portfolio(updated_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_portfolio_trading_date ON cs2_portfolio(trading_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_portfolio_market_type ON cs2_portfolio(market_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_decision_portfolio_id ON cs2_decision(portfolio_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_decision_trading_date ON cs2_decision(trading_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_decision_item_name ON cs2_decision(item_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_decision_market_type ON cs2_decision(market_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_signal_portfolio_id ON cs2_signal(portfolio_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_signal_analyst ON cs2_signal(analyst)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_signal_item_name ON cs2_signal(item_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cs2_signal_market_type ON cs2_signal(market_type)')

    conn.commit()
    conn.close()
    print(f"CS2 database initialized: {CS2_DB_PATH}")

if __name__ == "__main__":
    init_cs2_database()
