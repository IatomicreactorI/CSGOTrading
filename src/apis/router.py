"""Router for APIs"""

from apis.steam.api import SteamAPI
from apis.cs2market.api import CS2MarketAPI
from apis.reddit.api import RedditAPI

class APISource:
    CS2_MARKET = "cs2_market"
    REDDIT = "reddit"
    STEAM = "steam"

class Router():
    """Router for APIs"""
    
    def __init__(self, source: APISource):
        if source == APISource.CS2_MARKET:
            self.api = CS2MarketAPI()
        elif source == APISource.REDDIT:
            self.api = RedditAPI()
        elif source == APISource.STEAM:
            self.api = SteamAPI()
        else:
            raise ValueError(f"Invalid API source: {source}")

    
    def get_cs2_stock_daily_candles_df(self, ticker, trading_date):
        """Get CS2 stock daily candles dataframe."""
        return self.api.get_cs2_stock_daily_candles_df(ticker=ticker, trading_date=trading_date)
    
    def get_cs2_stock_last_close_price(self, ticker, trading_date):
        """Get CS2 stock last close price."""
        return self.api.get_cs2_last_close_price(ticker=ticker, trading_date=trading_date)
    
    def get_reddit_posts(self, subreddits, limit=25, time_filter="week", sort="hot", trading_date=None):
        """Get posts from Reddit subreddits."""
        return self.api.get_subreddit_posts(
            subreddits=subreddits,
            limit=limit,
            time_filter=time_filter,
            sort=sort,
            trading_date=trading_date
        )
    
    def search_reddit_posts(self, query, subreddits, limit=25, sort="relevance", trading_date=None):
        """Search for posts in Reddit subreddits."""
        return self.api.search_posts(
            query=query,
            subreddits=subreddits,
            limit=limit,
            sort=sort,
            trading_date=trading_date
        )
    
    def get_ticker_relevant_reddit_posts(self, ticker, subreddits, limit=15, min_score=2, min_comments=1, trading_date=None):
        """Get Reddit posts relevant to a specific CS2 ticker/item."""
        return self.api.get_ticker_relevant_posts(
            ticker=ticker,
            subreddits=subreddits,
            limit=limit,
            min_score=min_score,
            min_comments=min_comments,
            trading_date=trading_date
        )

    # Steam API wrappers
    def get_steam_item_news(self, ticker, trading_date=None, limit=15):
        """Get Steam news relevant to a CS2 item."""
        return self.api.get_item_relevant_news(
            item_name=ticker,
            count=limit,
            trading_date=trading_date,
        )

    def get_steam_app_news(self, appid=SteamAPI.CS2_APP_ID, count=30, maxlength=300):
        """Get app-level Steam news (GetNewsForApp)."""
        return self.api.get_news_for_app(
            appid=appid,
            count=count,
            maxlength=maxlength,
        )

    def get_steam_historical_news(self, ticker, trading_date, window_days=7, limit=15, csv_path=None):
        """Load Steam news from historical CSV within a date window."""
        return self.api.get_historical_news_from_csv(
            ticker=ticker,
            trading_date=trading_date,
            window_days=window_days,
            limit=limit,
            csv_path=csv_path,
        )

