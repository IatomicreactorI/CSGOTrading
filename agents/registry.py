from typing import Dict, Callable, List
from agents.analysts import *
from agents.portfolio_manager import portfolio_agent
from graph.constants import AgentKey

class AgentRegistry:
    """Registry for all agents."""
    
    # Initialize as actual dictionaries, not just type annotations
    agent_func_mapping: Dict[str, Callable] = {}
    agent_doc_mapping: Dict[str, str] = {}

    # Analyst KEYs
    ANALYST_KEYS = [
        AgentKey.TECHNICAL, 
        AgentKey.SENTIMENT,
        AgentKey.SENTIMENT_REVERSE,
        AgentKey.LIQUIDITY,
        AgentKey.EVENT
    ]

    @classmethod
    def get_agent_func_by_key(cls, key: str) -> Callable:
        """Get agent function by key."""
        return cls.agent_func_mapping.get(key)

    @classmethod
    def get_all_analyst_keys(cls) -> List[str]:
        """Get all analyst keys."""
        return cls.ANALYST_KEYS
    
    @classmethod
    def check_agent_key(cls, key: str) -> bool:
        """Check if an agent key is valid."""
        return key in cls.ANALYST_KEYS

    @classmethod
    def get_analyst_info(cls, key: str) -> str:
        """Get analyst info."""
        return cls.agent_doc_mapping[key]

    @classmethod
    def register_agent(cls, key: str, agent_func: Callable, agent_doc: str) -> None:
        """
        Register a new agent.
        
        Args:
            key: Unique identifier for the agent
            agent_func: Function that implements the agent logic
            agent_doc: short description of the agent
        """
        cls.agent_func_mapping[key] = agent_func
        cls.agent_doc_mapping[key] = agent_doc

    @classmethod
    def run_registry(cls):
        """Run the registry."""

        cls.register_agent(
            key=AgentKey.PORTFOLIO,
            agent_func=portfolio_agent,
            agent_doc="Portfolio manager making final trading decisions based on the signals from the analysts."
        )
                
        cls.register_agent(
            key=AgentKey.TECHNICAL,
            agent_func=technical_agent,
            agent_doc="Technical analysis specialist using multiple technical analysis strategies."
        )

        cls.register_agent(
            key=AgentKey.SENTIMENT,
            agent_func=sentiment_agent,
            agent_doc="Sentiment analysis specialist analyzing Reddit community sentiment for CS2 market items."
        )

        cls.register_agent(
            key=AgentKey.SENTIMENT_REVERSE,
            agent_func=sentiment_reverse_agent,
            agent_doc="Reverse sentiment analysis specialist for CS2 market items. Uses contrarian hypothesis: when Reddit discussion is overly bullish, it may indicate market overheating and returns Bearish signal."
        )

        cls.register_agent(
            key=AgentKey.LIQUIDITY,
            agent_func=liquidity_agent,
            agent_doc="Liquidity analysis specialist analyzing market liquidity based on trading volume and Reddit engagement for CS2 market items."
        )

        cls.register_agent(
            key=AgentKey.EVENT,
            agent_func=event_agent,
            agent_doc="Event analysis specialist analyzing Steam official news and game updates for their impact on CS2 item prices (supply mechanism, visibility/popularity, market sentiment)."
        )
