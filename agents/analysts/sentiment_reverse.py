from graph.constants import AgentKey, Signal
from graph.schema import FundState, AnalystSignal
from agents.analysts.sentiment import sentiment_agent
from llm.prompt import SENTIMENT_REVERSE_PROMPT
from llm.inference import agent_call
from util.logger import logger
from util.cs2_db_helper import get_cs2_db

def sentiment_reverse_agent(state: FundState):
    """Contrarian sentiment: reverse Bullish/Bearish signals from sentiment_agent; Neutral stays Neutral."""
    agent_name = AgentKey.SENTIMENT_REVERSE
    ticker = state["ticker"]
    portfolio_id = state["portfolio"].id

    # Get db instance
    db = get_cs2_db()
    
    logger.log_agent_status(agent_name, ticker, "Analyzing Reddit sentiment (reverse mode)")
    
    # Call the original sentiment agent to get the base signal
    result = sentiment_agent(state)
    
    # Extract the signal from the result
    if "analyst_signals" not in result or not result["analyst_signals"]:
        # If sentiment agent didn't return a signal, return the original state
        return state
    
    original_signal = result["analyst_signals"][0]
    
    # Use LLM to generate reversed signal and justification
    llm_config = state["llm_config"]
    
    prompt = SENTIMENT_REVERSE_PROMPT.format(
        ticker=ticker,
        original_signal=original_signal.signal.value,
        original_justification=original_signal.justification
    )
    
    reversed_signal_obj = agent_call(
        prompt=prompt,
        llm_config=llm_config,
        pydantic_model=AnalystSignal,
    )
    
    # Log the reversed signal
    logger.log_signal(agent_name, ticker, reversed_signal_obj)
    
    # Save the reversed signal
    db.save_signal(portfolio_id, agent_name, ticker, prompt, reversed_signal_obj)
    
    return {"analyst_signals": [reversed_signal_obj]}

