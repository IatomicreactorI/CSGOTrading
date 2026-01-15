from graph.constants import AgentKey, Signal
from llm.prompt import (
    SENTIMENT_PROMPT,
    REDDIT_SENTIMENT_INSUFFICIENT_DATA_PROMPT,
    REDDIT_SENTIMENT_FETCH_ERROR_PROMPT,
)
from graph.schema import FundState, AnalystSignal
from llm.inference import agent_call
from apis.router import Router, APISource
from util.cs2_db_helper import get_cs2_db
from util.logger import logger

# Sentiment analysis thresholds
thresholds = {
    "reddit_post_count": 25,
    "reddit_subreddits": ["GlobalOffensiveTrade", "csgomarketforum", "cs2"],
    "reddit_time_filter": "week",
    "reddit_sort": "hot",
    # Reddit quality filtering thresholds
    "reddit_min_score": 2,  # Minimum post score for quality filtering
    "reddit_min_comments": 1,  # Minimum number of comments
    "reddit_relevant_limit": 15  # Limit for ticker-relevant posts
}


def sentiment_agent(state: FundState):
    """
    Sentiment analysis specialist analyzing Reddit community sentiment for CS2 market items.
    This agent focuses on community discussions, sentiment, and market sentiment trends.
    """
    agent_name = AgentKey.SENTIMENT
    ticker = state["ticker"]
    trading_date = state["trading_date"]
    llm_config = state["llm_config"]
    portfolio_id = state["portfolio"].id
    exp_name = state["exp_name"]
    
    db = get_cs2_db()
    
    logger.log_agent_status(agent_name, ticker, "Fetching Reddit market sentiment")
    
    # Get the Reddit posts
    router = Router(APISource.REDDIT)
    
    try:
        # Use ticker-relevant search instead of generic subreddit hot posts
        # Pass trading_date to filter posts (only posts from trading_date - 7 days to trading_date)
        reddit_posts = router.get_ticker_relevant_reddit_posts(
            ticker=ticker,
            subreddits=thresholds["reddit_subreddits"],
            limit=thresholds["reddit_relevant_limit"],
            min_score=thresholds["reddit_min_score"],
            min_comments=thresholds["reddit_min_comments"],
            trading_date=trading_date
        )
        
        # If no posts or too few posts, use insufficient-data prompt
        min_posts = thresholds["reddit_post_count"]
        if not reddit_posts or len(reddit_posts) < min_posts:
            post_count = len(reddit_posts) if reddit_posts else 0
            logger.warning(f"Insufficient Reddit posts for {ticker}: {post_count} < {min_posts}. Using insufficient-data prompt.")

            prompt = REDDIT_SENTIMENT_INSUFFICIENT_DATA_PROMPT.format(
                ticker=ticker,
                post_count=post_count,
                min_posts=min_posts
            )
            
            signal = agent_call(
                prompt=prompt,
                llm_config=llm_config,
                pydantic_model=AnalystSignal,
            )
            
            logger.log_signal(agent_name, ticker, signal)
            db.save_signal(portfolio_id, agent_name, ticker, prompt, signal)
            
            return {"analyst_signals": [signal]}
        
        logger.info(f"Found {len(reddit_posts)} ticker-relevant Reddit posts for {ticker}. Proceeding with LLM analysis.")
                
    except Exception as e:
        logger.error(f"Failed to fetch Reddit sentiment for {ticker}: {e}")

        prompt = REDDIT_SENTIMENT_FETCH_ERROR_PROMPT.format(ticker=ticker)
        
        signal = agent_call(
            prompt=prompt,
            llm_config=llm_config,
            pydantic_model=AnalystSignal,
        )

        logger.log_signal(agent_name, ticker, signal)
        db.save_signal(portfolio_id, agent_name, ticker, prompt, signal)

        return {"analyst_signals": [signal]}
    
    # Process Reddit posts
    reddit_posts_dict = [m.model_dump_json() for m in reddit_posts]
    
    prompt = SENTIMENT_PROMPT.format(
        ticker=ticker,
        reddit_posts=reddit_posts_dict,
        post_count=len(reddit_posts)
    )
    
    logger.info(f"Using {len(reddit_posts)} ticker-relevant Reddit posts for {ticker} sentiment analysis")
    
    # Get LLM signal
    signal = agent_call(
        prompt=prompt,
        llm_config=llm_config,
        pydantic_model=AnalystSignal,
    )

    # save signal
    logger.log_signal(agent_name, ticker, signal)
    db.save_signal(portfolio_id, agent_name, ticker, prompt, signal)

    return {"analyst_signals": [signal]}

