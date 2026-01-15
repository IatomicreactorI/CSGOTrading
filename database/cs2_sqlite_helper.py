import sqlite3
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional
from graph.schema import Decision, AnalystSignal
from database.interface import BaseDB
from database.cs2_sqlite_setup import CS2_DB_PATH
from util.logger import logger

class CS2SQLiteDB(BaseDB):
    """The SQLite database assistant dedicated to the CS2 market."""
    
    def __init__(self):
        self.db_path = CS2_DB_PATH

    def _get_connection(self):
        """Get a database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row # access columns by name
        return conn

    def _parse_positions(self, positions_str):
        """Parse positions JSON string safely."""
        if not positions_str or not positions_str.strip():
            return {}
        
        try:
            # Debug logging
            logger.debug(f"Parsing positions: '{positions_str}' (type: {type(positions_str)})")
            result = json.loads(positions_str)
            logger.debug(f"Parsed result: {result} (type: {type(result)})")
            return result
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse positions JSON: '{positions_str}' - {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error parsing positions: '{positions_str}' - {e}")
            return {}

    def get_config(self, config_id: str) -> Optional[Dict]:
        """Get CS2 config by id."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM cs2_config WHERE id = ?', (config_id,))
            row = cursor.fetchone()
            
            if row:
                return row
            return None
        except Exception as e:
            logger.error(f"Error getting CS2 config: {e}")
            return None
        finally:
            if conn:
                conn.close()
            
    def get_config_id_by_name(self, exp_name: str) -> Optional[str]:
        """Get CS2 config id by experiment name."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM cs2_config WHERE exp_name = ?', (exp_name,))
            row = cursor.fetchone()
            
            if row:
                return row['id']
            return None
        except Exception as e:
            logger.error(f"CS2 config not found: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def create_config(self, config: Dict) -> Optional[str]:
        """Create a new CS2 config entry."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            config_id = str(uuid.uuid4())
            cursor.execute('''
                INSERT INTO cs2_config (id, exp_name, updated_at, items, has_planner, llm_model, llm_provider, market_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                config_id,
                config["exp_name"],
                datetime.now(timezone.utc).isoformat(), # UTC time
                json.dumps(config["tickers"]),  # CS2 items list
                config["planner_mode"],
                config["llm"]["model"],
                config["llm"]["provider"],
                'cs2'
            ))
            
            conn.commit()
            return config_id
        except Exception as e:
            logger.error(f"Error creating CS2 config: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_latest_trading_date(self, config_id: str) -> Optional[datetime]:
        """Get the latest trading date for a CS2 config."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT trading_date FROM cs2_portfolio 
                WHERE config_id = ? AND trading_date IS NOT NULL
                ORDER BY updated_at DESC 
                LIMIT 1
            ''', (config_id,))
            
            row = cursor.fetchone()
            
            if row:
                return datetime.fromisoformat(row['trading_date'])
            return None
        except Exception as e:
            logger.error(f"Error getting latest CS2 trading date: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_latest_portfolio(self, config_id: str) -> Optional[Dict]:
        """Get the latest CS2 portfolio for a config."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM cs2_portfolio 
                WHERE config_id = ? 
                ORDER BY updated_at DESC 
                LIMIT 1
            ''', (config_id,))
            
            row = cursor.fetchone()
            
            if row:
                # Convert sqlite3.Row to dict
                portfolio_dict = {
                    'id': row['id'],
                    'config_id': row['config_id'],
                    'updated_at': row['updated_at'],
                    'trading_date': row['trading_date'],
                    'cashflow': row['cashflow'],
                    'total_assets': row['total_assets'],
                    'positions': self._parse_positions(row['positions']),
                    'market_type': row['market_type']
                }
                return portfolio_dict
            return None
        except Exception as e:
            logger.error(f"Error getting latest CS2 portfolio: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_portfolio(self, portfolio_id: str) -> Optional[Dict]:
        """Get CS2 portfolio by ID."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM cs2_portfolio 
                WHERE id = ?
            ''', (portfolio_id,))
            
            row = cursor.fetchone()
            
            if row:
                # Convert sqlite3.Row to dict
                portfolio_dict = {
                    'id': row['id'],
                    'config_id': row['config_id'],
                    'updated_at': row['updated_at'],
                    'trading_date': row['trading_date'],
                    'cashflow': row['cashflow'],
                    'total_assets': row['total_assets'],
                    'positions': self._parse_positions(row['positions']),
                    'market_type': row['market_type']
                }
                return portfolio_dict
            return None
        except Exception as e:
            logger.error(f"Error getting CS2 portfolio: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def create_portfolio(self, config_id: str, cashflow: float, trading_date: datetime) -> Optional[Dict]:
        """Create a new CS2 portfolio entry."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            portfolio_id = str(uuid.uuid4())
            cursor.execute('''
                INSERT INTO cs2_portfolio (id, config_id, updated_at, trading_date, cashflow, total_assets, positions, market_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                portfolio_id,
                config_id,
                datetime.now(timezone.utc).isoformat(),
                trading_date.isoformat(),
                cashflow,
                cashflow,
                json.dumps({}),  # Empty positions
                'cs2'
            ))
            
            conn.commit()
            return {
                'id': portfolio_id,
                'config_id': config_id,
                'cashflow': cashflow,
                'total_assets': cashflow,
                'positions': {},
                'trading_date': trading_date.isoformat(),
                'market_type': 'cs2'
            }
        except Exception as e:
            logger.error(f"Error creating CS2 portfolio: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def copy_portfolio(self, config_id: str, portfolio: Dict, trading_date: datetime) -> Optional[Dict]:
        """Copy CS2 portfolio to a new trading date.
        
        If a portfolio already exists for the given config_id and trading_date,
        returns the existing portfolio instead of creating a duplicate.
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Check if portfolio already exists for this config_id and trading_date
            cursor.execute('''
                SELECT * FROM cs2_portfolio 
                WHERE config_id = ? AND trading_date = ?
                LIMIT 1
            ''', (config_id, trading_date.isoformat()))
            
            existing_row = cursor.fetchone()
            
            if existing_row:
                # Portfolio already exists, return the existing one
                logger.info(f"Portfolio already exists for config_id={config_id}, trading_date={trading_date.isoformat()}, returning existing portfolio")
                return {
                    'id': existing_row['id'],
                    'config_id': existing_row['config_id'],
                    'updated_at': existing_row['updated_at'],
                    'trading_date': existing_row['trading_date'],
                    'cashflow': existing_row['cashflow'],
                    'total_assets': existing_row['total_assets'],
                    'positions': self._parse_positions(existing_row['positions']),
                    'market_type': existing_row['market_type']
                }
            
            # No existing portfolio, create a new one
            portfolio_id = str(uuid.uuid4())
            # Handle positions data - it might already be JSON string or dict
            positions_data = portfolio['positions']
            if isinstance(positions_data, str):
                # Already a JSON string, use as is
                positions_json = positions_data
            else:
                # It's a dict, convert to JSON
                positions_json = json.dumps(positions_data)
            
            # Calculate total_assets from cashflow and positions
            if isinstance(positions_data, str):
                # If positions is a JSON string, parse it first
                positions_dict = json.loads(positions_data)
                total_assets = portfolio['cashflow'] + sum(
                    position['value'] for position in positions_dict.values()
                )
            else:
                # If positions is already a dict
                total_assets = portfolio['cashflow'] + sum(
                    position['value'] for position in positions_data.values()
                )
            
            cursor.execute('''
                INSERT INTO cs2_portfolio (id, config_id, updated_at, trading_date, cashflow, total_assets, positions, market_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                portfolio_id,
                config_id,
                datetime.now(timezone.utc).isoformat(),
                trading_date.isoformat(),
                portfolio['cashflow'],
                total_assets,
                positions_json,
                'cs2'
            ))
            
            conn.commit()
            return {
                'id': portfolio_id,
                'config_id': config_id,
                'cashflow': portfolio['cashflow'],
                'total_assets': total_assets,
                'positions': positions_data if isinstance(positions_data, dict) else json.loads(positions_data),
                'trading_date': trading_date.isoformat(),
                'market_type': 'cs2'
            }
        except Exception as e:
            logger.error(f"Error copying CS2 portfolio: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def update_portfolio(self, config_id: str, portfolio: Dict, trading_date: datetime) -> bool:
        """Update CS2 portfolio."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Calculate total_assets from cashflow and positions
            positions_data = portfolio['positions']
            if isinstance(positions_data, str):
                # If positions is a JSON string, parse it first
                positions_dict = json.loads(positions_data)
                total_assets = portfolio['cashflow'] + sum(
                    position['value'] for position in positions_dict.values()
                )
            else:
                # If positions is already a dict
                total_assets = portfolio['cashflow'] + sum(
                    position['value'] for position in portfolio['positions'].values()
                )
            
            cursor.execute('''
                UPDATE cs2_portfolio 
                SET cashflow = ?, total_assets = ?, positions = ?, updated_at = ?
                WHERE config_id = ? AND trading_date = ?
            ''', (
                portfolio['cashflow'],
                total_assets,
                json.dumps(portfolio['positions']),
                datetime.now(timezone.utc).isoformat(),
                config_id,
                trading_date.isoformat()
            ))
            
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating CS2 portfolio: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def save_decision(self, portfolio_id: str, ticker: str, prompt: str, decision, trading_date: datetime) -> Optional[str]:
        """Save CS2 trading decision."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            decision_id = str(uuid.uuid4())
            
            # Handle decision - it might be a Pydantic model or dict
            if hasattr(decision, 'action'):
                # It's a Pydantic model
                action = decision.action
                shares = decision.shares
                price = decision.price
                justification = decision.justification
            else:
                # It's a dict
                action = decision['action']
                shares = decision['shares']
                price = decision['price']
                justification = decision['justification']
            
            cursor.execute('''
                INSERT INTO cs2_decision (id, portfolio_id, updated_at, trading_date, item_name, llm_prompt, action, quantity, price, justification, market_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                decision_id,
                portfolio_id,
                datetime.now(timezone.utc).isoformat(),
                trading_date.isoformat(),
                ticker,  # CS2 item name
                prompt,
                action,
                shares,
                price,
                justification,
                'cs2'
            ))
            
            conn.commit()
            return decision_id
        except Exception as e:
            logger.error(f"Error saving CS2 decision: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def save_signal(self, portfolio_id: str, analyst: str, ticker: str, prompt: str, signal) -> Optional[str]:
        """Save CS2 analyst signal."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            signal_id = str(uuid.uuid4())
            
            # Handle signal - it might be a Pydantic model or dict
            if hasattr(signal, 'signal'):
                # It's a Pydantic model
                signal_value = signal.signal
                justification = signal.justification
            else:
                # It's a dict
                signal_value = signal['signal']
                justification = signal['justification']
            
            cursor.execute('''
                INSERT INTO cs2_signal (id, portfolio_id, updated_at, item_name, llm_prompt, analyst, signal, justification, market_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                signal_id,
                portfolio_id,
                datetime.now(timezone.utc).isoformat(),
                ticker,  # CS2 item name
                prompt,
                analyst,
                signal_value,
                justification,
                'cs2'
            ))
            
            conn.commit()
            return signal_id
        except Exception as e:
            logger.error(f"Error saving CS2 signal: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_recent_portfolio_ids_by_config_id(self, config_id: str, limit: int) -> List[str]:
        """Get recent CS2 portfolio IDs by config ID."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id FROM cs2_portfolio 
                WHERE config_id = ? 
                ORDER BY updated_at DESC 
                LIMIT ?
            ''', (config_id, limit))
            
            return [row['id'] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting recent CS2 portfolio IDs: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def get_decision_memory(self, exp_name: str, ticker: str, limit: int) -> List[Dict]:
        """Get CS2 decision memory for an item."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Step 1: Get config id by exp_name
            config_id = self.get_config_id_by_name(exp_name)
            if not config_id:
                logger.error(f"Config not found for {exp_name}")
                return []
            
            # Step 2: Get recent 5 portfolio transactions
            portfolio_ids = self.get_recent_portfolio_ids_by_config_id(config_id, limit)
            if not portfolio_ids:
                logger.error(f"Portfolio not found for {config_id}")
                return []
            
            # Step 3: Get decision memory by portfolio ids and ticker
            
                
            # Create the correct number of placeholders for the IN clause
            placeholders = ','.join('?' * len(portfolio_ids))
            query = f'''
                SELECT * FROM cs2_decision 
                WHERE portfolio_id IN ({placeholders}) AND item_name = ?
                ORDER BY updated_at DESC 
                LIMIT ?
            '''
                
            # Combine portfolio_ids and ticker into parameters
            params = portfolio_ids + [ticker, limit]
            cursor.execute(query, params)
                
            decisions = []
            for row in cursor.fetchall():
                decisions.append({
                    'trading_date': row['trading_date'],
                    'action': row['action'],
                    'quantity': row['quantity'],
                    'price': row['price']
                })
                
            return decisions
        except Exception as e:
            logger.error(f"Error getting CS2 decision memory: {e}")
            return []
        finally:
            if conn:
                conn.close()



            

