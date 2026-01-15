"""
Reddit API client implementation for CS2 market sentiment analysis.
Uses praw (Python Reddit API Wrapper) to fetch posts from CS2-related subreddits.
Supports loading historical data from CSV file.
"""

import praw
import os
import requests
import pandas as pd
from typing import List, Optional
from datetime import datetime, timedelta
from apis.common_model import MediaNews
from util.logger import logger


class RedditAPI:
    """Reddit API Wrapper for CS2 market sentiment."""
    
    # CS2 weapon name synonyms and abbreviations mapping (including common community nicknames)
    WEAPON_SYNONYMS = {
        'AK-47': ['AK', 'AK47', 'AK 47', 'Kalashnikov', 'Kalash'],
        'M4A4': ['M4', 'M4A4', 'M4A4-S'],
        'M4A1-S': ['M4A1', 'M4A1S', 'M4A1-S', 'M4A1 S', 'M4S', '消音M4', 'Silenced M4'],
        'AWP': ['AWP', '大狙', '鸟狙', 'AWP Sniper'],
        'Desert Eagle': ['DE', 'Deagle', 'Desert Eagle', 'DesertEagle', '沙鹰', 'Desert Eagle .50'],
        'Desert Eagle |': ['DE', 'Deagle', '沙鹰'],
    }
    
    # CS2 skin name variants and nicknames (including common community aliases)
    SKIN_VARIANTS = {
        # AWP skins
        'Hyper Beast': ['Hyper Beast', 'HyperBeast', 'HB', 'Hyper', 'Beast', 'HyperBeast AWP'],
        'Asiimov': ['Asiimov', 'Asimov', 'Asi', 'AWP Asiimov', 'Asiimov AWP'],
        
        # AK-47 skins
        'Bloodsport': ['Bloodsport', 'Blood Sport', 'Blood', 'Sport', 'AK Bloodsport'],
        'Asiimov': ['Asiimov', 'Asimov', 'Asi', 'AK Asiimov', 'Asiimov AK'],
        
        # M4A4 skins
        'Neo-Noir': ['Neo-Noir', 'NeoNoir', 'Neo Noir', 'Neo', 'Noir', 'M4 Neo-Noir'],
        'Desolate Space': ['Desolate Space', 'DesolateSpace', 'Desolate', 'Space', 'M4 Desolate'],
        'Dragon King': ['Dragon King', 'DragonKing', 'Dragon', 'King', '龍王', '龙王', 'M4 Dragon King'],
        
        # M4A1-S skins
        'Decimator': ['Decimator', 'M4A1 Decimator', 'Decimator M4'],
        'Leaded Glass': ['Leaded Glass', 'LeadedGlass', 'Leaded', 'Glass', 'M4A1 Leaded'],
        
        # Desert Eagle skins
        'Printstream': ['Printstream', 'Print Stream', 'Print', 'Stream', 'DE Printstream', 'Printstream DE'],
        'Mecha Industries': ['Mecha Industries', 'MechaIndustries', 'Mecha', 'Industries', 'DE Mecha'],
        
        # Sticker related
        'Team Liquid': ['Team Liquid', 'TeamLiquid', 'TL', 'Liquid', 'TL Holo'],
        'FaZe Clan': ['FaZe Clan', 'FaZeClan', 'FaZe', 'Faze', 'FaZe Holo', 'Faze Holo'],
        'Taste Buddy': ['Taste Buddy', 'TasteBuddy', 'Taste', 'Buddy', 'Taste Buddy Holo'],
        'Bolt Energy': ['Bolt Energy', 'BoltEnergy', 'Bolt', 'Energy', 'Bolt Energy Foil'],
        'Hypnoteyes': ['Hypnoteyes', 'Hypnotic Eyes', 'Hypnotic', 'Eyes', 'Hypnoteyes Holo'],
        
        # Case related
        'Broken Fang': ['Broken Fang', 'BrokenFang', 'Broken', 'Fang', 'BF', 'Broken Fang Case'],
        'Riptide': ['Riptide', 'Riptide Case', 'Operation Riptide'],
        'Wildfire': ['Wildfire', 'Wildfire Case', 'Operation Wildfire'],
        'Dreams & Nightmares': ['Dreams & Nightmares', 'Dreams and Nightmares', 'Dreams', 'Nightmares', 'D&N Case', 'DN Case'],
    }
    
    # Team/organization abbreviation mapping
    TEAM_ABBREVIATIONS = {
        'Team Liquid': ['TL', 'Liquid'],
        'FaZe Clan': ['FaZe', 'Faze', 'FaZe Clan'],
        'Paris 2023': ['Paris', 'Paris 2023', 'P2023'],
    }
    
    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None, 
                 user_agent: Optional[str] = None, csv_path: Optional[str] = None):
        """
        Initialize Reddit API client.
        
        Args:
            client_id: Reddit client ID (optional, can use env vars)
            client_secret: Reddit client secret (optional, can use env vars)
            user_agent: User agent string (optional, defaults to 'cs2-market-sentiment/1.0')
            csv_path: Path to historical Reddit data CSV file (optional)
        """
        # Disable proxies (if system proxies are configured but unavailable)
        session = requests.Session()
        session.proxies = {}  # Disable proxies
        
        self.reddit = praw.Reddit(
            client_id=client_id or os.getenv("REDDIT_CLIENT_ID", ""),
            client_secret=client_secret or os.getenv("REDDIT_CLIENT_SECRET", ""),
            user_agent=user_agent or os.getenv("REDDIT_USER_AGENT", "cs2-market-sentiment/1.0"),
            requestor_kwargs={"session": session}
        )
        # CSV path for historical data
        if csv_path is None:
            csv_path = os.path.join(os.path.dirname(__file__), 'reddit_data.csv')
        self.csv_path = csv_path
        self._csv_data = None  # Cache for CSV data
    
    def _load_reddit_data_from_csv(self) -> Optional[pd.DataFrame]:
        """
        Load historical Reddit data from a CSV file.
        
        Returns:
            DataFrame with columns: publish_time, title, publisher, link, summary, score, num_comments, subreddit
            Returns None if CSV file doesn't exist or can't be loaded
        """
        if self._csv_data is not None:
            return self._csv_data
        
        if not os.path.exists(self.csv_path):
            logger.warning(f"Reddit historical data CSV not found: {self.csv_path}")
            return None
        
        try:
            df = pd.read_csv(self.csv_path)
            # Parse publish_time
            df['publish_time'] = pd.to_datetime(df['publish_time'])
            # Cache the data
            self._csv_data = df
            logger.debug(f"Loaded {len(df)} Reddit posts from CSV: {self.csv_path}")
            return df
        except Exception as e:
            logger.error(f"Failed to load Reddit data from CSV {self.csv_path}: {e}")
            return None
    
    def _get_posts_from_csv(self,
                            subreddits: List[str],
                            start_timestamp: Optional[float] = None,
                            end_timestamp: Optional[float] = None,
                            query: Optional[str] = None,
                            min_score: int = 0,
                            min_comments: int = 0,
                            limit: int = 25) -> List[MediaNews]:
        """
        Fetch posts from a CSV file.
        
        Args:
            subreddits: List of subreddit names.
            start_timestamp: Start timestamp.
            end_timestamp: End timestamp.
            query: Search keyword (optional).
            min_score: Minimum score.
            min_comments: Minimum number of comments.
            limit: Maximum number of returned posts.
            
        Returns:
            List of MediaNews objects.
        """
        df = self._load_reddit_data_from_csv()
        if df is None:
            return []
        
        # Filter by subreddit
        if 'subreddit' in df.columns:
            df = df[df['subreddit'].isin([s.replace('r/', '') for s in subreddits])]
        elif 'publisher' in df.columns:
            # Extract subreddit name from publisher column
            df = df[df['publisher'].str.contains('|'.join(subreddits), case=False, na=False)]
        
        # Filter by time range
        if start_timestamp and end_timestamp:
            start_date = datetime.fromtimestamp(start_timestamp)
            end_date = datetime.fromtimestamp(end_timestamp)
            df = df[(df['publish_time'] >= start_date) & (df['publish_time'] <= end_date)]
        
        # Search by keyword
        if query:
            query_lower = query.lower()
            df = df[
                df['title'].str.contains(query_lower, case=False, na=False) |
                df['summary'].str.contains(query_lower, case=False, na=False)
            ]
        
        # Quality filtering
        if 'score' in df.columns:
            df = df[df['score'] >= min_score]
        if 'num_comments' in df.columns:
            df = df[df['num_comments'] >= min_comments]
        
        # Sort by score (if available)
        if 'score' in df.columns:
            df = df.sort_values('score', ascending=False)
        else:
            df = df.sort_values('publish_time', ascending=False)
        
        # Limit number of rows
        df = df.head(limit)
        
        # Convert to MediaNews objects
        news_list = []
        for _, row in df.iterrows():
            news_item = MediaNews(
                title=row['title'],
                publish_time=row['publish_time'].strftime("%Y-%m-%d %H:%M:%S"),
                publisher=row.get('publisher', f"r/{row.get('subreddit', 'unknown')}"),
                link=row.get('link', ''),
                summary=row.get('summary', ''),
                score=int(row.get('score', 0)) if pd.notna(row.get('score', 0)) else None,
                num_comments=int(row.get('num_comments', 0)) if pd.notna(row.get('num_comments', 0)) else None
            )
            news_list.append(news_item)
        
        return news_list
    
    def get_subreddit_posts(self, 
                           subreddits: List[str], 
                           limit: int = 25,
                           time_filter: str = "week",
                           sort: str = "hot",
                           trading_date: Optional[datetime] = None) -> List[MediaNews]:
        """
        Get posts from specified subreddits.
        
        Args:
            subreddits: List of subreddit names (e.g., ['GlobalOffensiveTrade', 'csgomarketforum'])
            limit: Maximum number of posts per subreddit
            time_filter: Time filter ('all', 'year', 'month', 'week', 'day', 'hour')
            sort: Sort method ('hot', 'top', 'new', 'rising')
            trading_date: Trading date to filter posts (only posts from trading_date - 7 days to trading_date)
            
        Returns:
            List of MediaNews objects filtered by trading_date
        """
        # If trading_date is provided, calculate the date range (trading_date - 7 days to trading_date)
        # NOTE: We ONLY read from CSV to avoid data leakage (using future data in backtesting)
        # Reddit API cannot fetch historical data for specific dates, so we must use pre-fetched CSV data
        if trading_date:
            if isinstance(trading_date, str):
                trading_date = datetime.fromisoformat(trading_date.replace('Z', '+00:00'))
            elif hasattr(trading_date, 'date'):
                # Convert to datetime if it's a date object
                trading_date = datetime.combine(trading_date, datetime.min.time())
            
            # Set time to end of day for trading_date
            trading_date_end = trading_date.replace(hour=23, minute=59, second=59)
            # Start date is 7 days before trading_date
            start_date = trading_date_end - timedelta(days=7)
            start_timestamp = start_date.timestamp()
            end_timestamp = trading_date_end.timestamp()
        else:
            start_timestamp = None
            end_timestamp = None
        
        # Read from CSV (trading_date is required)
        # If CSV has no data, return an empty list (do not fetch from Reddit API in real time to avoid data leakage)
        if not trading_date or not start_timestamp or not end_timestamp:
            logger.warning("trading_date is required to fetch Reddit posts from CSV")
            return []
        
        csv_posts = self._get_posts_from_csv(
            subreddits=subreddits,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            limit=limit
        )
        
        if csv_posts:
            logger.debug(f"Using {len(csv_posts)} posts from CSV for historical date {trading_date.date()}")
            return csv_posts[:limit]
        else:
            logger.warning(f"No CSV data found for {trading_date.date()}")
            return []
    
    def search_posts(self, 
                    query: str, 
                    subreddits: List[str],
                    limit: int = 25,
                    sort: str = "relevance",
                    min_score: int = 0,
                    min_comments: int = 0,
                    trading_date: Optional[datetime] = None) -> List[MediaNews]:
        """
        Search for posts matching query in specified subreddits with quality filtering.
        
        Args:
            query: Search query
            subreddits: List of subreddit names to search in
            limit: Maximum number of results
            sort: Sort method ('relevance', 'hot', 'top', 'new', 'comments')
            min_score: Minimum post score (default: 0, no filter)
            min_comments: Minimum number of comments (default: 0, no filter)
            trading_date: Trading date to filter posts (only posts from trading_date - 7 days to trading_date)
            
        Returns:
            List of MediaNews objects, filtered by quality and trading_date
        """
        # If trading_date is provided, calculate the date range (trading_date - 7 days to trading_date)
        trading_date_end = None
        if trading_date:
            if isinstance(trading_date, str):
                trading_date = datetime.fromisoformat(trading_date.replace('Z', '+00:00'))
            elif hasattr(trading_date, 'date'):
                # Convert to datetime if it's a date object
                trading_date = datetime.combine(trading_date, datetime.min.time())
            
            # Set time to end of day for trading_date
            trading_date_end = trading_date.replace(hour=23, minute=59, second=59)
            # Start date is 7 days before trading_date
            start_date = trading_date_end - timedelta(days=7)
            start_timestamp = start_date.timestamp()
            end_timestamp = trading_date_end.timestamp()
        else:
            start_timestamp = None
            end_timestamp = None
        
        # Read from CSV (trading_date is required)
        if not trading_date or not start_timestamp or not end_timestamp:
            logger.warning("trading_date is required to fetch Reddit posts from CSV")
            return []
        
        csv_posts = self._get_posts_from_csv(
            subreddits=subreddits,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            query=query,
            min_score=min_score,
            min_comments=min_comments,
            limit=limit
        )
        
        if csv_posts:
            logger.debug(f"Using {len(csv_posts)} posts from CSV for search query '{query}' on {trading_date.date()}")
            return csv_posts[:limit]
        else:
            return []
    
    def _expand_keywords_with_synonyms(self, keywords: List[str]) -> List[str]:
        """
        Expand the keyword list using synonym mappings.
        
        Args:
            keywords: Original keyword list.
            
        Returns:
            Expanded keyword list (including synonyms and variants).
        """
        expanded = []
        
        for keyword in keywords:
            # Add original keyword
            expanded.append(keyword)
            
            # Check weapon synonyms
            for weapon, synonyms in self.WEAPON_SYNONYMS.items():
                if weapon in keyword or keyword in weapon:
                    expanded.extend(synonyms)
                    break
            
            # Check skin variants
            for skin, variants in self.SKIN_VARIANTS.items():
                if skin in keyword or keyword in skin:
                    expanded.extend(variants)
                    break
            
            # Check team abbreviations
            for team, abbreviations in self.TEAM_ABBREVIATIONS.items():
                if team in keyword or keyword in team:
                    expanded.extend(abbreviations)
                    break
        
        # Remove duplicates and preserve order
        seen = set()
        result = []
        for kw in expanded:
            kw_lower = kw.lower()
            if kw_lower not in seen:
                seen.add(kw_lower)
                result.append(kw)
        
        return result
    
    def get_ticker_relevant_posts(self,
                                 ticker: str,
                                 subreddits: List[str],
                                 limit: int = 15,
                                 min_score: int = 2,
                                 min_comments: int = 1,
                                 trading_date: Optional[datetime] = None) -> List[MediaNews]:
        """
        Get Reddit posts relevant to a specific CS2 ticker/item with expanded synonym search.
        
        This method extracts keywords from the ticker name, expands them with synonyms/nicknames,
        and searches for relevant discussions using multiple query variations.
        
        Args:
            ticker: CS2 item name (e.g., "AK-47 | Asiimov (Factory New)")
            subreddits: List of subreddit names to search in
            limit: Maximum number of results
            min_score: Minimum post score for quality filtering (default: 2)
            min_comments: Minimum number of comments (default: 1)
            trading_date: Trading date to filter posts (only posts from trading_date - 7 days to trading_date)
            
        Returns:
            List of MediaNews objects relevant to the ticker, filtered by trading_date
        """
        # Extract search keywords from ticker
        # For items like "AK-47 | Asiimov (Factory New)", extract "AK-47", "Asiimov"
        # For items like "Sticker | Team Liquid (Holo) | Paris 2023", extract "Team Liquid", "Paris 2023"
        base_keywords = []
        
        # Split by | and take main parts
        parts = ticker.split('|')
        if len(parts) > 0:
            # First part often contains weapon/item name
            first_part = parts[0].strip()
            base_keywords.append(first_part)
        
        # Extract skin/sticker name (usually in second part if exists)
        if len(parts) > 1:
            second_part = parts[1].strip()
            # Remove common suffixes like "(Factory New)", "(Holo)", etc.
            second_part = second_part.split('(')[0].strip()
            if second_part:
                base_keywords.append(second_part)
        
        # Extract third part if exists (e.g., "Paris 2023" in stickers)
        if len(parts) > 2:
            third_part = parts[2].strip()
            third_part = third_part.split('(')[0].strip()
            if third_part:
                base_keywords.append(third_part)
        
        # Expand keywords using synonyms
        expanded_keywords = self._expand_keywords_with_synonyms(base_keywords)
        
        # Build search queries - try different combinations with expanded keywords
        search_queries = []
        
        # 1. Use skin/sticker names (most specific, prioritize expanded variants)
        if len(base_keywords) > 1:
            skin_keyword = base_keywords[1]
            skin_expanded = [kw for kw in expanded_keywords if skin_keyword.lower() in kw.lower() or kw.lower() in skin_keyword.lower()]
            if skin_expanded:
                # Use the first 5 most relevant skin variants
                search_queries.extend(skin_expanded[:5])
        
        # 2. Weapon + skin combinations (using expanded keywords)
        if len(base_keywords) >= 2:
            weapon_keywords = [kw for kw in expanded_keywords if base_keywords[0].lower() in kw.lower()]
            skin_keywords = [kw for kw in expanded_keywords if base_keywords[1].lower() in kw.lower()]
            
            # Generate combined queries (up to 10 combinations)
            for weapon in weapon_keywords[:3]:
                for skin in skin_keywords[:3]:
                    search_queries.append(f"{weapon} {skin}")
                    search_queries.append(f"{weapon}{skin}")  # Combination without space
                    if len(search_queries) >= 10:
                        break
                if len(search_queries) >= 10:
                    break
        
        # 3. If only weapon name is present (case type, etc.)
        if len(base_keywords) == 1:
            search_queries.extend(expanded_keywords[:5])
        
        # 4. If there are keywords but no queries generated, use original keywords
        if not search_queries and base_keywords:
            search_queries.extend(base_keywords)
        
        # Remove duplicate queries
        search_queries = list(dict.fromkeys(search_queries))
        
        # Limit the number of queries (maximum 15)
        search_queries = search_queries[:15]
        
        logger.debug(f"Search queries for {ticker}: {search_queries[:5]}... (total: {len(search_queries)})")
        
        all_posts = []
        seen_titles = set()  # Deduplicate posts by title
        
        # Search with each query and combine results
        for query in search_queries:
            posts = self.search_posts(
                query=query,
                subreddits=subreddits,
                limit=limit * 2,  # Fetch more results; they will be filtered and sorted later
                sort="relevance",
                min_score=min_score,
                min_comments=min_comments,
                trading_date=trading_date
            )
            
            for post in posts:
                if post.title not in seen_titles:
                    all_posts.append(post)
                    seen_titles.add(post.title)
        
        # Sort by relevance (posts with ticker name or synonyms in title first) and limit
        def relevance_score(post: MediaNews) -> int:
            score = 0
            ticker_lower = ticker.lower()
            title_lower = post.title.lower()
            summary_lower = (post.summary or "").lower()
            text = f"{title_lower} {summary_lower}"
            
            # Exact ticker match (highest score)
            if ticker_lower in text:
                score += 50
            
            # Check matches for base keywords
            base_matches = sum(1 for kw in base_keywords if kw.lower() in text)
            if base_matches > 0:
                score += base_matches * 10
            
            # Check matches for expanded keywords (synonyms)
            expanded_matches = sum(1 for kw in expanded_keywords if kw.lower() in text)
            if expanded_matches > 0:
                score += expanded_matches * 5
            
            # Keywords appearing in the title (more important than in the body)
            if any(kw.lower() in title_lower for kw in base_keywords):
                score += 15
            if any(kw.lower() in title_lower for kw in expanded_keywords[:10]):  # Only check the first 10 expanded keywords
                score += 5
            
            return score
        
        all_posts.sort(key=relevance_score, reverse=True)
        return all_posts[:limit]

