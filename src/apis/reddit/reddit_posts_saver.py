"""
Save Reddit posts to CSV file for later review.
"""

import os
import pandas as pd
from datetime import datetime
from typing import List
from apis.common_model import MediaNews
from util.logger import logger


def save_reddit_posts_to_csv(
    posts: List[MediaNews],
    ticker: str,
    trading_date: datetime,
    data_source_type: str,
    exp_name: str = None,
    csv_path: str = None
):
    """
    Save Reddit posts to CSV file.
    
    Args:
        posts: List of MediaNews objects (Reddit posts)
        ticker: CS2 item name
        trading_date: Trading date
        data_source_type: "ticker-relevant" or "general"
        exp_name: Experiment name (optional, used to create separate CSV files per experiment)
        csv_path: Path to CSV file (default: src/apis/reddit/reddit_posts_{exp_name}.csv or reddit_posts.csv)
    """
    if csv_path is None:
        # Create separate CSV file for each experiment
        if exp_name:
            # Sanitize exp_name for filename (remove special characters)
            safe_exp_name = "".join(c for c in exp_name if c.isalnum() or c in ('-', '_')).strip()
            csv_filename = f'reddit_posts_{safe_exp_name}.csv'
        else:
            csv_filename = 'reddit_posts.csv'
        csv_path = os.path.join(os.path.dirname(__file__), csv_filename)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # Prepare data rows
    rows = []
    for post in posts:
        row = {
            'trading_date': trading_date.strftime('%Y-%m-%d'),
            'ticker': ticker,
            'data_source_type': data_source_type,
            'title': post.title,
            'publish_time': post.publish_time,
            'publisher': post.publisher,
            'link': post.link or '',
            'summary': post.summary or ''
        }
        rows.append(row)
    
    if not rows:
        logger.debug(f"No posts to save for {ticker} on {trading_date.date()}")
        return
    
    # Create DataFrame
    df = pd.DataFrame(rows)
    
    # Append to existing CSV or create new one
    if os.path.exists(csv_path):
        # Read existing CSV
        existing_df = pd.read_csv(csv_path)
        # Append new rows
        df = pd.concat([existing_df, df], ignore_index=True)
        # Remove duplicates (same trading_date, ticker, title, publish_time)
        df = df.drop_duplicates(subset=['trading_date', 'ticker', 'title', 'publish_time'], keep='last')
    
    # Save to CSV
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved {len(rows)} Reddit posts to {csv_path} for {ticker} on {trading_date.date()} (data source: {data_source_type})")

