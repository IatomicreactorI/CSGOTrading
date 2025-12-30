import math
import pandas as pd
from graph.schema import FundState, AnalystSignal
from graph.constants import Signal, AgentKey
from llm.prompt import TECHNICAL_PROMPT
from llm.inference import agent_call
from apis.router import Router, APISource
from util.cs2_db_helper import get_cs2_db
from util.logger import logger

# Technical Thresholds
thresholds = {
    "trend": {
        "short": 8,
        "medium": 21,
        "long": 52,
    },
    "mean_reversion": {
        "bollinger_window": 20,
        "rolling_window": 50,
        "z_score_extreme": 2.0,
        "bb_position_threshold": 0.2
    },
    "rsi": {
        "period": 14,
        "bullish": 30,
        "bearish": 70,
    },
    "volatility": {
        "bullish": 0.8,
        "bearish": 1.2,
    },
    "volume": {
        "trend": 20,
        "correlation": 20,
        "unusual_volume": 2.0,
    },
    "support_resistance": {
        "pivot_window": 5,
        "lookback_period": 20,
    }

}


def technical_agent(state: FundState):
    """Technical analysis specialist that excels at short to medium-term price movement predictions."""
    agent_name = AgentKey.TECHNICAL
    ticker = state["ticker"]
    trading_date = state["trading_date"]
    llm_config = state["llm_config"]
    portfolio_id = state["portfolio"].id
    exp_name = state["exp_name"]
    
    # Get db instance
    db = get_cs2_db()
    
    logger.log_agent_status(agent_name, ticker, "Analyzing price data")

    # Get the price data
    router = Router(APISource.CS2_MARKET)
    
    try:
        prices_df = router.get_cs2_stock_daily_candles_df(ticker=ticker, trading_date=trading_date)
    except Exception as e:
        logger.error(f"Failed to fetch price data for {ticker}: {e}")
        return state

    # Analyze technical indicators
    signal_results = {
        "trend": get_trend_signal(prices_df, thresholds["trend"]),
        "mean_reversion": get_mean_reversion_signal(prices_df, thresholds["mean_reversion"]),
        "rsi": get_rsi_signal(prices_df, thresholds["rsi"]),
        "volatility":  get_volatility_signal(prices_df, thresholds["volatility"]),
        "volume": get_volume_analysis(prices_df, thresholds["volume"]),
        "price_levels": get_support_resistance(prices_df, thresholds["support_resistance"]),
    }

    # Make prompt
    prompt = TECHNICAL_PROMPT.format(
        ticker=ticker,
        analysis=signal_results
    )

    # Get LLM signal
    signal = agent_call(
        prompt=prompt,
        llm_config=llm_config,
        pydantic_model=AnalystSignal
    )

    # save signal
    logger.log_signal(agent_name, ticker, signal)
    db.save_signal(portfolio_id, agent_name, ticker, prompt, signal)

    return {"analyst_signals": [signal]}


def get_trend_signal(prices_df, params):
    """Advanced trend following strategy using multiple timeframes and indicators"""

    def _calculate_ema(prices_df, window):
        return prices_df["close"].ewm(span=window, adjust=False).mean()

    # Calculate EMAs for multiple timeframes
    ema_short = _calculate_ema(prices_df, params["short"])
    ema_medium = _calculate_ema(prices_df, params["medium"])
    ema_long = _calculate_ema(prices_df, params["long"])

    # Determine trend direction and strength
    short_trend = ema_short > ema_medium
    medium_trend = ema_medium > ema_long

    if short_trend.iloc[-1] and medium_trend.iloc[-1]:
        signal = Signal.BULLISH
    elif not short_trend.iloc[-1] and not medium_trend.iloc[-1]:
        signal = Signal.BEARISH
    else:
        signal = Signal.NEUTRAL

    return signal


def get_mean_reversion_signal(prices_df, params):
    """Mean reversion strategy using statistical measures and Bollinger Bands"""
    
    def _calculate_bollinger_bands(prices_df: pd.DataFrame, window: int) -> tuple[pd.Series, pd.Series]:
        sma = prices_df["close"].rolling(window).mean()
        std_dev = prices_df["close"].rolling(window).std()
        upper_band = sma + (std_dev * 2)
        lower_band = sma - (std_dev * 2)
        return upper_band, lower_band

    # Calculate Bollinger Bands with configured window
    bb_upper, bb_lower = _calculate_bollinger_bands(prices_df, params["bollinger_window"])

    # Calculate z-score with configured rolling window
    ma = prices_df["close"].rolling(window=params["rolling_window"]).mean()
    std = prices_df["close"].rolling(window=params["rolling_window"]).std()
    z_score = (prices_df["close"] - ma) / std

    # Calculate normalized position within Bollinger Bands
    price_vs_bb = (prices_df["close"].iloc[-1] - bb_lower.iloc[-1]) / (bb_upper.iloc[-1] - bb_lower.iloc[-1])

    # Use threshold values for signal conditions
    if z_score.iloc[-1] < params["z_score_extreme"] and price_vs_bb < params["bb_position_threshold"]:
        signal = Signal.BULLISH
    elif z_score.iloc[-1] > params["z_score_extreme"] and price_vs_bb > (1 - params["bb_position_threshold"]):
        signal = Signal.BEARISH
    else:
        signal = Signal.NEUTRAL

    return signal


def get_rsi_signal(prices_df, params):
    """RSI signal that indicate overbought/oversold conditions"""

    def _calculate_rsi(prices_df: pd.DataFrame, period: int) -> pd.Series:
        delta = prices_df["close"].diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    rsi = _calculate_rsi(prices_df, params["period"])
    if rsi.iloc[-1] > params["bearish"]:
        signal = Signal.BEARISH
    elif rsi.iloc[-1] < params["bullish"]:
        signal = Signal.BULLISH
    else:
        signal = Signal.NEUTRAL

    return signal


def get_volatility_signal(prices_df, params):
    """Volatility-based trading strategy"""
    # Calculate various volatility metrics
    returns = prices_df["close"].pct_change()

    # Historical volatility
    hist_vol = returns.rolling(21).std() * math.sqrt(252)

    # Volatility regime detection
    vol_ma = hist_vol.rolling(63).mean()
    vol_regime = hist_vol / vol_ma

    # Volatility mean reversion
    vol_z_score = (hist_vol - vol_ma) / hist_vol.rolling(63).std()

    # Generate signal based on volatility regime
    current_vol_regime = vol_regime.iloc[-1]
    vol_z = vol_z_score.iloc[-1]

    if current_vol_regime < params["bullish"] and vol_z < -1:
        # Low vol regime, potential for expansion
        signal = Signal.BULLISH
    elif current_vol_regime > params["bearish"] and vol_z > 1:
        # High vol regime, potential for contraction
        signal = Signal.BEARISH
    else:
        signal = Signal.NEUTRAL

    return signal


def get_volume_analysis(prices_df, params):
    """Analyze volume characteristics"""
    volume = prices_df['volume']
    price = prices_df['close']
    
    # Calculate volume moving average
    vol_ma = volume.rolling(window=params["trend"]).mean()
    
    # Calculate price-volume relationship
    price_volume_corr = price.rolling(window=params["correlation"]).corr(volume)
    
    # Calculate volume trend
    vol_trend = (volume > vol_ma.shift(1)).astype(int)
    
    result = f"- Volume trend: {Signal.BULLISH if vol_trend.iloc[-1] == 1 else Signal.BEARISH}\n"
    result += f"- Price-volume correlation: {price_volume_corr.iloc[-1]}\n"
    result += f"- Unusual volume: {volume.iloc[-1] > (vol_ma.iloc[-1] * params['unusual_volume'])}\n"
    return result


def get_support_resistance(prices_df, params):
    """Calculate support and resistance levels"""
    def _is_level(prices: pd.Series, i: int, level_type: str, pivot_window: int = params["pivot_window"]) -> bool:
        """Check if the price point is a support/resistance level by comparing with surrounding prices"""
        start_idx = max(0, i - pivot_window)
        end_idx = min(len(prices), i + pivot_window + 1)
        window_prices = prices.iloc[start_idx:end_idx]
        current_price = prices.iloc[i]
        
        left_prices = window_prices.iloc[:pivot_window]
        right_prices = window_prices.iloc[pivot_window+1:]
        
        if level_type == 'support':
            return (len(left_prices[left_prices > current_price]) >= 2 and 
                   len(right_prices[right_prices > current_price]) >= 2)
        elif level_type == 'resistance':
            return (len(left_prices[left_prices < current_price]) >= 2 and 
                   len(right_prices[right_prices < current_price]) >= 2)
        # else:
        #     raise ValueError("level_type must be 'support' or 'resistance'")
    
    def _find_levels(prices: pd.Series, lookback_period: int = params["lookback_period"]):
        levels = []
        for i in range(lookback_period, len(prices)):
            if _is_level(prices, i, 'support'):
                levels.append((i, prices.iloc[i]))
            elif _is_level(prices, i, 'resistance'):
                levels.append((i, prices.iloc[i]))
        return levels
    
    price_data = prices_df['close']
    current_price = price_data.iloc[-1]
    levels = _find_levels(price_data)
    
    support_levels = [price for _, price in levels if price < current_price]
    resistance_levels = [price for _, price in levels if price > current_price]
    
    support = max(support_levels) if support_levels else None
    resistance = min(resistance_levels) if resistance_levels else None

    if support is None or resistance is None:
        return "Failed to analyze support and resistance levels"
    else:
        result = f"- Current price: {current_price}\n"
        result += f"- Nearest support: {support}\n"
        result += f"- Nearest resistance: {resistance}\n"
        result += f"- Price to support: {(current_price - support) / support}\n"
        result += f"- Price to resistance: {(resistance - current_price) / current_price}\n"
        return result
