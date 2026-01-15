from typing import List, Dict, Any
from pydantic import BaseModel, Field
from agents.registry import AgentRegistry
from graph.constants import AgentKey
from llm.prompt import PLANNER_PROMPT
from llm.inference import agent_call
from util.logger import logger


class PlannerOutput(BaseModel):
    """Pydantic model for planner agent output."""
    analysts: List[str] = Field(
        description="Name list of selected analysts"
    )
    justification: str = Field(
        description="Explanation for the analyst selection",
        default="No justification provided due to error"
    )

def planner_agent(ticker: str, llm_config: Dict[str, Any], workflow_analysts: List[str]) -> List[str]:
    """
    Planner agent that decides which analysts to use based on self-knowledge.
    It functions as a pre-requisite for the agentic workflow.
    """
    
    logger.log_agent_status(AgentKey.PLANNER, ticker, "Planning")
    analyst_info = [
        {"analyst_name": key,
         "analyst_info": AgentRegistry.get_analyst_info(key)
         } for key in workflow_analysts]

    prompt = PLANNER_PROMPT.format(
        ticker=ticker,
        analysts=analyst_info
    )

    result = agent_call(
        prompt=prompt,
        llm_config=llm_config,
        pydantic_model=PlannerOutput
    )

    logger.info(f"Planner agent selected {result.analysts} | Justification: {result.justification}")
    return result.analysts