"""
CS2 Market API client implementation for CS2 market sentiment analysis.
Uses CS2 Market API to fetch market data.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta


def load_cs2_data(csv_path=None, fill_missing=True):
    """
    Load CS2 market data and optionally fill missing calendar dates.

    Args:
        csv_path: Path to the CSV file.
        fill_missing: Whether to fill missing dates using neighboring days.
    """
    if csv_path is None:
        csv_path = os.path.join(os.path.dirname(__file__), 'cs2_data.csv')

    df = pd.read_csv(csv_path)

    # Detect date column with a fixed priority: batch_id -> date -> timestamp -> time -> dt
    detected_date_column = None
    for candidate in ['batch_id', 'date', 'timestamp', 'time', 'dt']:
        if candidate in df.columns:
            detected_date_column = candidate
            break

    if detected_date_column is None:
        raise ValueError(
            f"CSV missing date column. Required columns: batch_id, date, timestamp, time, dt. File: {csv_path}"
        )

    # Parse date column and normalize to a unified 'date' column.
    # If the original column is batch_id, keep it (date only) and create 'date'.
    if detected_date_column == 'batch_id':
        df['batch_id'] = pd.to_datetime(df[detected_date_column], errors='coerce')
        # batch_id keeps only the date part (no time)
        df['batch_id'] = df['batch_id'].dt.date
        df['date'] = pd.to_datetime(df['batch_id'])
    else:
        df[detected_date_column] = pd.to_datetime(df[detected_date_column], errors='coerce')
        df = df.rename(columns={detected_date_column: 'date'})
        # If batch_id does not exist, create it from 'date' (date only)
        if 'batch_id' not in df.columns:
            df['batch_id'] = df['date'].dt.date

    # Ensure open/close/volume are numeric
    for numeric_col in ['open', 'close', 'volume']:
        if numeric_col in df.columns:
            df[numeric_col] = pd.to_numeric(df[numeric_col], errors='coerce')

    # Optionally fill missing dates per item
    if fill_missing and 'name' in df.columns:
        df = _fill_missing_dates(df)
    
    return df


def _fill_missing_dates(df):
    """
    Fill missing calendar dates per item using nearby observations.

    Strategy:
        - Prefer average of values from the previous and next available dates.
        - Fallback to previous-only or next-only values when only one side exists.
    """
    filled_rows = []
    
    # Process by item name
    for item_name, item_df in df.groupby('name'):
        item_df = item_df.sort_values('date').copy()
        
        # Build full date range for this item
        min_date = item_df['date'].min().date()
        max_date = item_df['date'].max().date()
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        
        # Find missing dates
        existing_dates = set(item_df['date'].dt.date)
        missing_dates = [d.date() for d in date_range if d.date() not in existing_dates]
        
        # Create filled rows for each missing date
        for missing_date in missing_dates:
            missing_datetime = pd.to_datetime(missing_date)
            
            # Look up data from two days before and after
            prev_date = missing_datetime - pd.Timedelta(days=2)
            next_date = missing_datetime + pd.Timedelta(days=2)
            
            prev_data = item_df[item_df['date'].dt.date == prev_date.date()]
            next_data = item_df[item_df['date'].dt.date == next_date.date()]
            
            # If both sides exist, use the mean of the two days
            if len(prev_data) > 0 and len(next_data) > 0:
                prev_row = prev_data.iloc[0]
                next_row = next_data.iloc[0]
                
                # Create a new row using the mean of neighbors
                new_row = prev_row.copy()
                new_row['date'] = missing_datetime
                
                # Set batch_id to the missing date (date only)
                if 'batch_id' in new_row.index:
                    new_row['batch_id'] = missing_date
                
                # Fill numeric columns
                for col in ['open', 'close', 'volume']:
                    if col in new_row.index:
                        prev_val = prev_row[col]
                        next_val = next_row[col]
                        if pd.notna(prev_val) and pd.notna(next_val):
                            new_row[col] = (prev_val + next_val) / 2
                        elif pd.notna(prev_val):
                            new_row[col] = prev_val
                        elif pd.notna(next_val):
                            new_row[col] = next_val
                
                filled_rows.append(new_row)
            # If only previous day exists, copy previous values
            elif len(prev_data) > 0:
                prev_row = prev_data.iloc[0]
                new_row = prev_row.copy()
                new_row['date'] = missing_datetime
                # Set batch_id
                if 'batch_id' in new_row.index:
                    new_row['batch_id'] = missing_datetime.replace(hour=12, minute=0, second=0)
                filled_rows.append(new_row)
            # If only next day exists, copy next values
            elif len(next_data) > 0:
                next_row = next_data.iloc[0]
                new_row = next_row.copy()
                new_row['date'] = missing_datetime
                # Set batch_id
                if 'batch_id' in new_row.index:
                    new_row['batch_id'] = missing_datetime.replace(hour=12, minute=0, second=0)
                filled_rows.append(new_row)
    
    # Append filled rows back to the original DataFrame
    if filled_rows:
        filled_df = pd.DataFrame(filled_rows)
        df = pd.concat([df, filled_df], ignore_index=True)
        df = df.sort_values(['name', 'date']).reset_index(drop=True)

    return df


def get_cs2_stock_daily_candles_df(ticker="cs2_weapons", trading_date=None):
    """
    Return CS2 item OHLCV history in stock-market compatible format.

    Follows stock-market logic: returns all history from the earliest date up to trading_date.
    Args:
        ticker: Item name used to filter a specific CS2 asset.
        trading_date: Trading date; data strictly after this date is excluded.
    Returns:
        DataFrame with columns: date, open, high, low, close, volume.
    """
    # Load raw CS2 data
    df = load_cs2_data()
    
    # Filter by item name
    df = df[df['name'] == ticker]
    if df.empty:
        print(f"Warning: no data found for item {ticker}")
        return pd.DataFrame()
    
    # Apply trading_date filter if provided (stock-style: exclude future data)
    if trading_date:
        if isinstance(trading_date, str):
            trading_date = pd.to_datetime(trading_date)
        
        # Keep rows with date <= trading_date
        df = df[df['date'] <= trading_date]
        
        if df.empty:
            print(f"Warning: no data found on or before {trading_date.date()}")
            return pd.DataFrame()
    
    # Ensure expected column names
    df = df.rename(columns={
        'open': 'open',
        'close': 'close',
        'volume': 'volume'
    })
    
    # Add synthetic high/low columns (CS2 data has only open/close)
    df['high'] = df[['open', 'close']].max(axis=1)
    df['low'] = df[['open', 'close']].min(axis=1)
    
    # Sort by date
    df = df.sort_values('date')
    
    return df


def get_cs2_last_close_price(ticker="cs2_weapons", trading_date=None):
    """
    Get the latest close price for a CS2 item up to trading_date.

    Returns:
        float: Latest close price, or 0.0 if no data.
    """
    df = get_cs2_stock_daily_candles_df(ticker, trading_date)
    
    if df.empty:
        return 0.0
    
    # Return close price on the latest available date
    latest_data = df.iloc[-1]
    return float(latest_data['close'])


class CS2MarketAPI:
    """Lightweight CS2 Market API wrapper used by the Router."""
    
    def get_cs2_stock_daily_candles_df(self, ticker, trading_date):
        """Get CS2 daily OHLCV dataframe."""
        return get_cs2_stock_daily_candles_df(ticker=ticker, trading_date=trading_date)
    
    def get_cs2_last_close_price(self, ticker, trading_date):
        """Get CS2 last close price up to trading_date."""
        return get_cs2_last_close_price(ticker=ticker, trading_date=trading_date)
