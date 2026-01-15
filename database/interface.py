from abc import ABC, abstractmethod
from datetime import datetime

class BaseDB(ABC):
    @abstractmethod
    def get_config(self, config_id: str) -> dict:
        pass

    @abstractmethod
    def get_config_id_by_name(self, exp_name: str) -> str:
        pass

    @abstractmethod
    def create_config(self, config: dict) -> str:
        pass

    @abstractmethod
    def get_latest_trading_date(self, config_id: str) -> datetime:
        pass

    @abstractmethod
    def get_latest_portfolio(self, config_id: str) -> dict:
        pass

    @abstractmethod
    def create_portfolio(self, config_id: str, cashflow: float, trading_date: datetime) -> str:
        pass

    @abstractmethod
    def copy_portfolio(self, config_id: str, portfolio: dict, trading_date: datetime) -> str:
        pass

    @abstractmethod
    def update_portfolio(self, config_id: str, portfolio: dict, trading_date: datetime) -> bool:
        pass

    @abstractmethod
    def save_decision(self, portfolio_id: str, ticker: str, prompt: str, decision: dict, trading_date: datetime) -> str:
        pass

    @abstractmethod
    def save_signal(self, portfolio_id: str, analyst: str, ticker: str, prompt: str, signal: dict) -> str:
        pass

    @abstractmethod
    def get_recent_portfolio_ids_by_config_id(self, config_id: str, limit: int) -> list:
        pass

    @abstractmethod
    def get_decision_memory(self, exp_name: str, ticker: str, limit: int) -> list:
        pass