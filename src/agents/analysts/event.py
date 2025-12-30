import os
import json
from typing import List
from graph.constants import AgentKey, Signal
from graph.schema import FundState, AnalystSignal
from apis.common_model import MediaNews
from llm.prompt import EVENT_PROMPT
from llm.inference import agent_call
from util.cs2_db_helper import get_cs2_db
from util.logger import logger
from apis.router import Router, APISource

# Event analysis thresholds
thresholds = {
    "steam_limit": 15,  # Maximum Steam news items to analyze
}


def event_agent(state: FundState):
    """
    Event analysis specialist for CS2 market items.
    Analyzes Steam official news and game updates for their impact on item prices.
    """
    agent_name = AgentKey.EVENT
    ticker = state["ticker"]
    trading_date = state["trading_date"]
    llm_config = state["llm_config"]
    portfolio_id = state["portfolio"].id
    exp_name = state["exp_name"]
    
    # Get db instance
    db = get_cs2_db()

    logger.log_agent_status(agent_name, ticker, "Analyzing Steam official news and events")

    # Load Steam news via Router (historical CSV, 7-day window)
    router = Router(APISource.STEAM)
    try:
        steam_news = router.get_steam_historical_news(
            ticker=ticker,
            trading_date=trading_date,
            window_days=7,
            limit=thresholds["steam_limit"],
        )
    except Exception as e:
        logger.error(f"Failed to fetch Steam news for {ticker} on {trading_date}: {e}")
        steam_news = []
    
    steam_json = json.dumps(
        [m.model_dump() for m in steam_news] if steam_news else [],
        ensure_ascii=False,
        indent=2
    )
    prompt = EVENT_PROMPT.format(
        ticker=ticker,
        steam_news=steam_json,
        news_count=len(steam_news) if steam_news else 0,
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

