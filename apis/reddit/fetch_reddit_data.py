"""
Script: Fetch Reddit data from the past year and save to CSV
Pre-fetch historical data to avoid calling Reddit API on every run
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
load_dotenv()

from apis.reddit.api import RedditAPI
from apis.common_model import MediaNews
from util.logger import logger

SUBREDDITS = ["GlobalOffensiveTrade", "csgomarketforum", "cs2"]
CSV_PATH = os.path.join(os.path.dirname(__file__), 'reddit_data.csv')
YEARS_BACK = 1  # How many years of data to fetch


def fetch_historical_reddit_posts(
    subreddits: List[str],
    start_date: datetime,
    end_date: datetime,
    posts_per_day: int = 50
) -> List[dict]:
    """
    Fetch Reddit posts within the specified date range
    
    Args:
        subreddits: List of subreddits
        start_date: Start date
        end_date: End date
        posts_per_day: Number of posts to fetch per day (estimated)
        
    Returns:
        List of posts, each post is a dictionary
    """
    # Temporarily disable proxy settings in environment variables (to avoid proxy connection issues)
    import os
    original_proxies = {}
    for proxy_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
        if proxy_var in os.environ:
            original_proxies[proxy_var] = os.environ[proxy_var]
            del os.environ[proxy_var]
    
    try:
        reddit_api = RedditAPI()
    finally:
        # Restore original proxy settings
        for proxy_var, value in original_proxies.items():
            os.environ[proxy_var] = value
    
    all_posts = []
    
    # Fetch data by week (to avoid fetching too much at once)
    current_date = start_date
    week_count = 0
    
    while current_date < end_date:
        week_end = min(current_date + timedelta(days=7), end_date)
        week_count += 1
        
        logger.info(f"Fetching data from {current_date.date()} to {week_end.date()} (Week {week_count})")
        
        # Calculate days from now to select appropriate time filter
        now = datetime.now()
        days_ago = (now - week_end).days
        
        if days_ago > 365:
            time_filter = "all"
            limit = 1000
        elif days_ago > 30:
            time_filter = "year"
            limit = 500
        elif days_ago > 7:
            time_filter = "month"
            limit = 300
        else:
            time_filter = "week"
            limit = 200
        
        # Fetch posts from each subreddit
        for subreddit_name in subreddits:
            try:
                subreddit = reddit_api.reddit.subreddit(subreddit_name)
                posts = subreddit.top(time_filter=time_filter, limit=limit)
                
                for post in posts:
                    # Check if post is within date range
                    post_date = datetime.fromtimestamp(post.created_utc)
                    if post_date < current_date or post_date >= week_end:
                        continue
                    
                    # Convert to dictionary
                    post_dict = {
                        'publish_time': post_date.strftime("%Y-%m-%d %H:%M:%S"),
                        'title': post.title,
                        'publisher': f"r/{subreddit_name}",
                        'link': post.url if post.url.startswith("http") else f"https://reddit.com{post.permalink}",
                        'summary': post.selftext[:500] if post.selftext else 'Link post',
                        'score': post.score,
                        'num_comments': post.num_comments,
                        'subreddit': subreddit_name,
                        'ticker': None,  
                    }
                    all_posts.append(post_dict)
                    
            except Exception as e:
                logger.error(f"Error fetching data from r/{subreddit_name}: {e}")
                continue
        
        current_date = week_end
        
        # Avoid rate limiting, add a small delay after fetching each week's data
        import time
        time.sleep(1)
    
    logger.info(f"Total posts fetched: {len(all_posts)}")
    return all_posts


def save_to_csv(posts: List[dict], csv_path: str):
    """
    Save post data to CSV file
    
    Args:
        posts: List of posts
        csv_path: CSV file path
    """
    if not posts:
        logger.warning("No post data to save")
        return
    
    df = pd.DataFrame(posts)
    
    # If file exists, append data and remove duplicates
    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path)
        df = pd.concat([existing_df, df], ignore_index=True)
        # Remove duplicates based on publish_time, title, publisher
        df = df.drop_duplicates(subset=['publish_time', 'title', 'publisher'], keep='last')
    
    # Sort by date
    df['publish_time'] = pd.to_datetime(df['publish_time'])
    df = df.sort_values('publish_time')
    
    # Save to CSV
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved {len(df)} posts to {csv_path}")


def main():
    """Main function: Fetch Reddit data from the past year"""
    logger.info("Starting to fetch historical Reddit data...")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * YEARS_BACK)
    
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    
    # Fetch data
    posts = fetch_historical_reddit_posts(
        subreddits=SUBREDDITS,
        start_date=start_date,
        end_date=end_date
    )
    
    # Save to CSV
    save_to_csv(posts, CSV_PATH)
    
    logger.info("Completed!")


if __name__ == "__main__":
    main()

