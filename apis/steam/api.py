"""
Steam API client implementation for CS2 market sentiment analysis.
Uses Steam Web API to fetch news and community discussions.
"""

import os
import requests
from typing import List, Optional
import pandas as pd
from datetime import datetime
from apis.common_model import MediaNews
from util.logger import logger


class SteamAPI:
    """Steam API Wrapper for CS2 market sentiment."""
    
    CS2_APP_ID = 730
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Steam API client.
        
        Args:
            api_key: Steam Web API key (optional, can use env vars)
        """
        self.api_key = api_key or os.getenv("STEAM_API_KEY", "")
        self.base_url = "https://api.steampowered.com"
        
    def get_news_for_app(self,
                        appid: int = CS2_APP_ID,
                        count: int = 10,
                        maxlength: int = 300) -> List[MediaNews]:
        """
        Get news for a Steam app using GetNewsForApp API.
        
        Args:
            appid: Steam App ID (default: CS2 = 730)
            count: Number of news items to return
            maxlength: Maximum length of news content
            
        Returns:
            List of MediaNews objects representing Steam news
        """
        try:
            url = f"{self.base_url}/ISteamNews/GetNewsForApp/v2/"
            params = {
                "appid": appid,
                "count": count,
                "maxlength": maxlength
            }
            
            # Steam API doesn't require API key for public endpoints
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"Steam API error: {response.status_code}")
                return []
            
            data = response.json()
            news_items = []
            
            app_news = data.get("appnews", {})
            news_list = app_news.get("newsitems", [])
            
            for item in news_list:
                # Convert Unix timestamp to ISO format
                date_str = datetime.fromtimestamp(item.get("date", 0)).isoformat()
                
                news = MediaNews(
                    title=item.get("title", ""),
                    publish_time=date_str,
                    publisher=item.get("author", "Steam"),
                    link=item.get("url", ""),
                    summary=item.get("contents", ""),
                    score=None,  # Steam news doesn't have score
                    num_comments=None  # Steam news doesn't have comment count
                )
                news_items.append(news)
            
            return news_items
            
        except Exception as e:
            logger.error(f"Error fetching Steam news: {e}")
            return []
    
    def get_item_relevant_news(self,
                               item_name: str,
                               count: int = 10,
                               trading_date: Optional[datetime] = None) -> List[MediaNews]:
        """
        Get Steam news relevant to a CS2 item.
        Filters news by item name keywords.
        
        Args:
            item_name: CS2 item name
            count: Number of news items
            trading_date: Trading date (filters by date)
            
        Returns:
            List of relevant news items
        """
        all_news = self.get_news_for_app(appid=self.CS2_APP_ID, count=count * 2)
        
        # Filter news by item name keywords
        keywords = item_name.lower().split()
        relevant_news = []
        
        for news in all_news:
            title_lower = news.title.lower()
            summary_lower = (news.summary or "").lower()
            
            # Check if any keyword appears in title or summary
            if any(keyword in title_lower or keyword in summary_lower for keyword in keywords):
                relevant_news.append(news)
                if len(relevant_news) >= count:
                    break
        
        return relevant_news

    def get_historical_news_from_csv(self,
                                     ticker: str,
                                     trading_date: datetime,
                                     window_days: int = 7,
                                     limit: int = 15,
                                     csv_path: Optional[str] = None) -> List[MediaNews]:
        """
        Load Steam news for a ticker from historical CSV within [trading_date - window_days, trading_date].
        """
        if csv_path is None:
            csv_path = os.path.join(os.path.dirname(__file__), "steam_data.csv")

        try:
            if isinstance(trading_date, str):
                trading_date = datetime.fromisoformat(trading_date.replace("Z", "+00:00"))
            end_dt = trading_date
            start_dt = end_dt - pd.Timedelta(days=window_days)

            if not os.path.exists(csv_path):
                logger.warning(f"Steam historical CSV not found: {csv_path}")
                return []

            df = pd.read_csv(csv_path)
            if "publish_time" in df.columns:
                df["publish_time"] = pd.to_datetime(df["publish_time"])

            mask_date = (df["publish_time"] >= start_dt) & (df["publish_time"] <= end_dt)
            mask_ticker = df["ticker"] == ticker
            sub_df = df[mask_date & mask_ticker]

            sub_df = sub_df.sort_values("publish_time", ascending=False).head(limit)

            news_list: List[MediaNews] = []
            for _, row in sub_df.iterrows():
                # Convert Timestamp to string if needed
                publish_time = row.get("publish_time", "")
                if isinstance(publish_time, pd.Timestamp):
                    publish_time = publish_time.strftime("%Y-%m-%d %H:%M:%S")
                elif not isinstance(publish_time, str):
                    publish_time = str(publish_time)
                
                news_list.append(
                    MediaNews(
                        title=row.get("title", ""),
                        publish_time=publish_time,
                        publisher=row.get("publisher", ""),
                        link=row.get("link", ""),
                        summary=row.get("summary", ""),
                        score=row.get("score", None) if pd.notna(row.get("score", None)) else None,
                        num_comments=row.get("num_comments", None) if pd.notna(row.get("num_comments", None)) else None,
                    )
                )
            return news_list
        except Exception as e:
            logger.warning(f"Failed to load Steam historical news for {ticker}: {e}")
            return []

