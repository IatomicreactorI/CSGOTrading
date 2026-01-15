import os
from typing import Dict, Any
from dataclasses import dataclass
from pydantic import BaseModel
from llm.provider import Provider
from util.logger import logger

@dataclass
class LLMConfig:
    """Configuration for LLM inference"""
    provider: str
    model: str
    temperature: float = 0.5
    max_retries: int = 3


def get_model(config: LLMConfig):
    """Get a model instance based on configuration."""

    provider = Provider(config.provider)
    model_config = provider.config

    if model_config.requires_api_key:
        api_key = os.getenv(model_config.env_key)
        if not api_key:
            logger.error(f"API Key Error: Please make sure {model_config.env_key} is set in your .env file.")
            raise ValueError(f"{provider} API key not found. Please set {model_config.env_key} in .env file.")
    
    kwargs = {
        "model": config.model,
        **({"api_key": api_key} if model_config.requires_api_key else {}),
        **({"base_url": model_config.base_url} if model_config.base_url else {}),
        **({"temperature": config.temperature} if config.temperature is not None else {})
    }
    
    try:
        return model_config.model_class(**kwargs)
    except Exception as e:
        logger.error(f"{provider} Chat Error: {e}")
        raise ValueError(f"{provider} Chat Error: {e}")

def agent_call(prompt: str, llm_config: Dict[str, Any], pydantic_model: BaseModel):
    """
    Makes an agent call with retry logic and structured output.
    
    Args:
        prompt: The prompt to send to the LLM
        llm_config: Configuration for the LLM
        output_model: The Pydantic model to use for structured output
    Returns:
        An instance of output_model (with defaults if error occurs)
    """
    llm_cfg = LLMConfig(**llm_config)
    llm = get_model(llm_cfg)

    # Explicitly use function_calling method for structured output
    llm = llm.with_structured_output(pydantic_model, method="function_calling")

    for attempt in range(llm_cfg.max_retries):
        try:
            result = llm.invoke(prompt)
            if result is None:
                raise ValueError("LLM returned None")
            return result
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{llm_cfg.max_retries} failed: {e}")
            if attempt == llm_cfg.max_retries - 1:
                logger.error(f"All {llm_cfg.max_retries} attempts failed")
                return pydantic_model()
    
    return pydantic_model() 