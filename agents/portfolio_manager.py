from graph.constants import AgentKey, Action
from llm.prompt import PORTFOLIO_PROMPT, PORTFOLIO_PROMPT_NO_FEE, RISK_CONTROL_PROMPT, RISK_CONTROL_PROMPT_DIRECT_LLM
from graph.schema import Decision, FundState, PositionRisk
from llm.inference import agent_call
from apis.router import Router, APISource
from util.cs2_db_helper import get_cs2_db
from util.logger import logger

# Portfolio Manager Thresholds
thresholds = {
    "decision_memory_limit": 5
}

# Trading friction assumptions, 2% per trade
TRANSACTION_FEE_RATE = 0.02 

def portfolio_agent(state: FundState):
    """Make final trading decisions and generate orders for a given weapon"""
    agent_name = AgentKey.PORTFOLIO
    portfolio = state["portfolio"]
    ticker = state["ticker"]
    exp_name = state["exp_name"]
    trading_date = state["trading_date"]
    analyst_signals = state["analyst_signals"]
    llm_config = state["llm_config"]
    num_tickers = state["num_tickers"]
    enable_transaction_fee = state.get("enable_transaction_fee", True)

    # Get db instance based on experiment name
    db = get_cs2_db()
    
    # Get price data
    router = Router(APISource.CS2_MARKET)
    try:
        current_price = router.get_cs2_stock_last_close_price(ticker=ticker, trading_date=trading_date)
    except Exception as e:
        logger.error(f"Failed to fetch price data for {ticker}: {e}")
        raise RuntimeError(f"Failed to make decision")
    
    # calculate the max position ratio
    max_position_ratio = 1
    if num_tickers > 1:
        # suppose a single ticker can occupy its own base allocation (1/N) plus that of one other ticker maximally, round to the nearest 0.05
        max_position_ratio = round(2 / num_tickers * 20) / 20
    

    # risk control
    # Use different prompts based on whether we have analyst signals
    if analyst_signals:
        # Traditional mode: use analyst signals
        risk_prompt = RISK_CONTROL_PROMPT.format(
            ticker_signals=analyst_signals,
            portfolio=portfolio.model_dump_json(),
            max_position_ratio=max_position_ratio,
        )
    else:
        # Direct LLM mode: analyze ticker directly without analyst signals
        risk_prompt = RISK_CONTROL_PROMPT_DIRECT_LLM.format(
            ticker=ticker,
            portfolio=portfolio.model_dump_json(),
            max_position_ratio=max_position_ratio,
        )

    position_risk = agent_call(
        prompt=risk_prompt,
        llm_config=llm_config,
        pydantic_model=PositionRisk,
    )
    
    logger.log_agent_status(agent_name, ticker, "Risk control")
    logger.log_risk(ticker, position_risk)

    # verify the position ratio if it is in the range
    if position_risk.optimal_position_ratio > max_position_ratio:
        # too bullish, set to the max
        position_risk.optimal_position_ratio = max_position_ratio
    elif position_risk.optimal_position_ratio < 0:
        # too bearish, set to 0
        position_risk.optimal_position_ratio = 0

    logger.log_agent_status(agent_name, ticker, "Making trading decisions")

    # Get decision memory
    decision_memory = db.get_decision_memory(exp_name, ticker, thresholds["decision_memory_limit"])
    current_shares, tradable_shares = calculate_ticker_shares(portfolio, current_price, ticker, position_risk.optimal_position_ratio)

    # make trading decision
    if enable_transaction_fee:
        prompt = PORTFOLIO_PROMPT.format(
            decision_memory=decision_memory,
            current_price=current_price,
            current_shares=current_shares,
            tradable_shares=tradable_shares,
            transaction_fee_rate=TRANSACTION_FEE_RATE,
            transaction_fee_rate_pct=TRANSACTION_FEE_RATE * 100,
        )
    else:
        # Use prompt without transaction fee mention
        prompt = PORTFOLIO_PROMPT_NO_FEE.format(
            decision_memory=decision_memory,
            current_price=current_price,
            current_shares=current_shares,
            tradable_shares=tradable_shares,
        )

    # Generate the trading decision
    ticker_decision = agent_call(
        prompt=prompt,
        llm_config=llm_config,
        pydantic_model=Decision
    )

    # post-process the decision due to possible reasoning error
    ticker_decision.price = current_price
    if ticker_decision.shares < 0 and ticker_decision.action == Action.SELL:
        ticker_decision.shares = -ticker_decision.shares
        
    # save decision
    logger.log_decision(ticker, ticker_decision)
    db.save_decision(portfolio.id, ticker, prompt, ticker_decision, trading_date)

    return {"decision": ticker_decision}


def calculate_ticker_shares(portfolio, current_price, ticker, optimal_position_ratio):
    """calculate the tradable shares for a given ticker based on portfolio"""

    # Get current position value (0 if no position exists)
    current_shares = 0 
    if ticker in portfolio.positions:
        current_shares = portfolio.positions[ticker].shares
    # current value for the ticker
    current_value = current_shares * current_price
    # total portfolio value
    total_portfolio_value = portfolio.cashflow + sum(portfolio.positions[t].value for t in portfolio.positions)
    # position limit for the ticker
    position_limit = total_portfolio_value * optimal_position_ratio
    # position value gap
    position_value_gap = position_limit - current_value

    if position_value_gap > 0: # still have room to buy, maximum tradable cash is the minor between position_value_gap and cashflow
        tradable_shares = min(position_value_gap, portfolio.cashflow) // current_price
    else: # need to sell, maximun selling shares is the minor between position gap and current shares
        tradable_shares = max(position_value_gap // current_price, -current_shares)
    
    return current_shares, tradable_shares
        

    
