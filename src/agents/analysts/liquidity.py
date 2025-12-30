from graph.constants import AgentKey, Signal
from graph.schema import FundState, AnalystSignal
from apis.router import Router, APISource
from llm.prompt import LIQUIDITY_PROMPT
from llm.inference import agent_call
from util.cs2_db_helper import get_cs2_db
from util.logger import logger

# Liquidity analysis thresholds
thresholds = {
    "volume": {
        "high": 100,  # High trading volume threshold
        "low": 10,    # Low trading volume threshold
    },
    "reddit": {
        "high_score": 50,      # High Reddit post score (upvotes)
        "low_score": 5,        # Low Reddit post score
        "high_comments": 20,   # High number of comments
        "low_comments": 2,     # Low number of comments
        "min_posts": 3,        # Minimum number of relevant posts for analysis
    },
    "reddit_subreddits": ["GlobalOffensiveTrade", "csgomarketforum", "cs2"],
    "reddit_relevant_limit": 15,
    "reddit_min_score": 0,
    "reddit_min_comments": 0,
}


def liquidity_agent(state: FundState):
    """Liquidity analysis specialist analyzing market liquidity based on trading volume and Reddit engagement for CS2 market items."""
    agent_name = AgentKey.LIQUIDITY
    ticker = state["ticker"]
    trading_date = state["trading_date"]
    llm_config = state["llm_config"]
    portfolio_id = state["portfolio"].id
    
    # Get db instance
    db = get_cs2_db()
    
    logger.log_agent_status(agent_name, ticker, "Analyzing market liquidity")
    
    # Initialize analysis results
    analysis_results = {
        "trading_volume": None,
        "reddit_engagement": None,
        "liquidity_score": None,
    }
    
    # 1. Get trading volume data
    try:
        router = Router(APISource.CS2_MARKET)
        prices_df = router.get_cs2_stock_daily_candles_df(ticker=ticker, trading_date=trading_date)
        
        if not prices_df.empty:
            # Get recent volume data (last 7 days)
            recent_df = prices_df.tail(7)
            avg_volume = recent_df['volume'].mean() if 'volume' in recent_df.columns else 0
            latest_volume = recent_df.iloc[-1]['volume'] if 'volume' in recent_df.columns else 0
            
            analysis_results["trading_volume"] = {
                "average_7d": float(avg_volume),
                "latest": float(latest_volume),
                "has_data": True,
            }
        else:
            analysis_results["trading_volume"] = {
                "average_7d": 0.0,
                "latest": 0.0,
                "has_data": False,
            }
    except Exception as e:
        logger.error(f"Failed to fetch trading volume for {ticker}: {e}")
        analysis_results["trading_volume"] = {
            "average_7d": 0.0,
            "latest": 0.0,
            "has_data": False,
        }
    
    # 2. Get Reddit engagement data
    try:
        reddit_router = Router(APISource.REDDIT)
        reddit_posts = reddit_router.get_ticker_relevant_reddit_posts(
            ticker=ticker,
            subreddits=thresholds["reddit_subreddits"],
            limit=thresholds["reddit_relevant_limit"],
            min_score=thresholds["reddit_min_score"],
            min_comments=thresholds["reddit_min_comments"],
            trading_date=trading_date
        )
        
        if reddit_posts and len(reddit_posts) >= thresholds["reddit"]["min_posts"]:
            # Calculate engagement metrics
            total_score = 0
            total_comments = 0
            valid_posts = 0
            
            for post in reddit_posts:
                if hasattr(post, 'score') and post.score is not None:
                    total_score += post.score
                if hasattr(post, 'num_comments') and post.num_comments is not None:
                    total_comments += post.num_comments
                valid_posts += 1
            
            avg_score = total_score / valid_posts if valid_posts > 0 else 0
            avg_comments = total_comments / valid_posts if valid_posts > 0 else 0
            
            analysis_results["reddit_engagement"] = {
                "post_count": len(reddit_posts),
                "average_score": float(avg_score),
                "average_comments": float(avg_comments),
                "total_score": float(total_score),
                "total_comments": float(total_comments),
                "has_data": True,
            }
        else:
            analysis_results["reddit_engagement"] = {
                "post_count": len(reddit_posts) if reddit_posts else 0,
                "average_score": 0.0,
                "average_comments": 0.0,
                "total_score": 0.0,
                "total_comments": 0.0,
                "has_data": False,
            }
    except Exception as e:
        logger.error(f"Failed to fetch Reddit engagement for {ticker}: {e}")
        analysis_results["reddit_engagement"] = {
            "post_count": 0,
            "average_score": 0.0,
            "average_comments": 0.0,
            "total_score": 0.0,
            "total_comments": 0.0,
            "has_data": False,
        }
    
    # 3. Format analysis data for LLM prompt
    volume_data = analysis_results["trading_volume"]
    reddit_data = analysis_results["reddit_engagement"]
    
    # Format trading volume analysis
    if volume_data["has_data"]:
        avg_volume = volume_data["average_7d"]
        latest_volume = volume_data["latest"]
        volume_status = "high" if avg_volume >= thresholds["volume"]["high"] else \
                       "low" if avg_volume < thresholds["volume"]["low"] else "moderate"
        trading_volume_analysis = (
            f"Trading volume data is available.\n"
            f"- 7-day average volume: {avg_volume:.0f}\n"
            f"- Latest volume: {latest_volume:.0f}\n"
            f"- Volume status: {volume_status.capitalize()}\n"
            f"- Assessment: {'High trading activity indicates good market liquidity' if volume_status == 'high' else 'Low trading activity indicates potential liquidity risk' if volume_status == 'low' else 'Moderate trading activity suggests acceptable liquidity'}"
        )
    else:
        trading_volume_analysis = (
            "Trading volume data is NOT available.\n"
            "- Assessment: No trading volume data indicates potential liquidity risk as we cannot assess market activity."
        )
    
    # Format Reddit engagement analysis
    if reddit_data["has_data"] and reddit_data["post_count"] >= thresholds["reddit"]["min_posts"]:
        avg_score = reddit_data["average_score"]
        avg_comments = reddit_data["average_comments"]
        total_score = reddit_data["total_score"]
        total_comments = reddit_data["total_comments"]
        post_count = reddit_data["post_count"]
        
        high_engagement = (
            avg_score >= thresholds["reddit"]["high_score"] or
            avg_comments >= thresholds["reddit"]["high_comments"]
        )
        low_engagement = (
            avg_score < thresholds["reddit"]["low_score"] and
            avg_comments < thresholds["reddit"]["low_comments"]
        )
        
        engagement_level = "high" if high_engagement else "low" if low_engagement else "moderate"
        
        reddit_engagement_analysis = (
            f"Reddit community engagement data is available.\n"
            f"- Number of relevant posts: {post_count}\n"
            f"- Average upvotes per post: {avg_score:.1f}\n"
            f"- Average comments per post: {avg_comments:.1f}\n"
            f"- Total upvotes: {total_score:.0f}\n"
            f"- Total comments: {total_comments:.0f}\n"
            f"- Engagement level: {engagement_level.capitalize()}\n"
            f"- Assessment: {'Strong community interest indicates active market and good liquidity' if engagement_level == 'high' else 'Weak community interest may indicate low market activity and liquidity risk' if engagement_level == 'low' else 'Moderate community interest suggests acceptable market activity'}"
        )
    else:
        post_count = reddit_data["post_count"]
        reddit_engagement_analysis = (
            f"Reddit community engagement data is INSUFFICIENT.\n"
            f"- Number of relevant posts found: {post_count}\n"
            f"- Minimum required: {thresholds['reddit']['min_posts']}\n"
            f"- Assessment: Insufficient Reddit data limits our ability to assess community interest and market sentiment. This may indicate low market visibility or limited community discussion."
        )
    
    # Create prompt for LLM
    prompt = LIQUIDITY_PROMPT.format(
        ticker=ticker,
        trading_volume_analysis=trading_volume_analysis,
        reddit_engagement_analysis=reddit_engagement_analysis,
        volume_high=thresholds["volume"]["high"],
        volume_low=thresholds["volume"]["low"],
        reddit_high_score=thresholds["reddit"]["high_score"],
        reddit_high_comments=thresholds["reddit"]["high_comments"],
        reddit_low_score=thresholds["reddit"]["low_score"],
        reddit_low_comments=thresholds["reddit"]["low_comments"],
        reddit_min_posts=thresholds["reddit"]["min_posts"]
    )
    
    # Get LLM signal
    signal = agent_call(
        prompt=prompt,
        llm_config=llm_config,
        pydantic_model=AnalystSignal
    )
    
    # Save signal
    logger.log_signal(agent_name, ticker, signal)
    db.save_signal(portfolio_id, agent_name, ticker, prompt, signal)
    
    return {"analyst_signals": [signal]}

