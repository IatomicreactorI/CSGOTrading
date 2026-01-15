# CSGOTrading: Multi-Agent LLM Framework for CS2 Market Trading

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![LangGraph](https://img.shields.io/badge/LangGraph-Enabled-green.svg)](https://github.com/langchain-ai/langgraph)
<!-- [![arXiv](https://img.shields.io/badge/arXiv-2501.xxxxx-b31b1b.svg)](https://arxiv.org/) -->

*An intelligent multi-agent system leveraging Large Language Models for automated Counter-Strike 2 market analysis and trading decisions*

<!-- [Paper](#citation) | [Documentation](#documentation) | [Installation](#installation) | [Quick Start](#quick-start) -->

</div>

---

## 📋 Abstract

**CSGOTrading** presents a novel multi-agent framework that applies Large Language Models (LLMs) to the domain of virtual item trading in Counter-Strike 2 (CS2) markets. Our system employs a hierarchical agent architecture built on LangGraph, incorporating specialized analyst agents (technical, sentiment, liquidity, and event-driven) coordinated by a meta-planner agent. The portfolio manager agent synthesizes multi-modal market signals to generate optimal trading decisions while accounting for transaction costs and risk constraints.

Key contributions:
- **Multi-Agent Architecture**: Modular design with specialized analyst agents for different market aspects
- **Dynamic Agent Selection**: Meta-planner agent adaptively selects relevant analysts based on market conditions
- **Multi-Source Data Integration**: Seamless aggregation of CS2 market data, Steam news, and Reddit sentiment
- **Configurable Workflow**: Support for both agentic workflows and direct LLM analysis modes
- **Risk-Aware Portfolio Management**: Sophisticated position sizing with transaction cost modeling

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CSGOTrading System                        │
└─────────────────────────────────────────────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                │      Workflow Engine          │
                │        (LangGraph)            │
                └───────────────┬───────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
┌───────▼───────┐      ┌───────▼───────┐      ┌───────▼───────┐
│    Planner    │      │   Analysts    │      │   Portfolio   │
│     Agent     │─────▶│    Agents     │─────▶│    Manager    │
└───────────────┘      └───────────────┘      └───────────────┘
                               │
                ┌──────────────┼──────────────┐
                │              │              │
        ┌───────▼──────┐ ┌────▼─────┐ ┌─────▼──────┐
        │  Technical   │ │Sentiment │ │ Liquidity  │
        │   Analysis   │ │ Analysis │ │  Analysis  │
        └──────────────┘ └──────────┘ └────────────┘
                │              │              │
        ┌───────▼──────┐ ┌────▼─────┐ ┌─────▼──────┐
        │    Event     │ │ Reverse  │ │    ...     │
        │   Analysis   │ │Sentiment │ │            │
        └──────────────┘ └──────────┘ └────────────┘
                                │
                    ┌───────────┴───────────┐
                    │    Data Sources       │
                    ├───────────────────────┤
                    │ • CS2 Market API      │
                    │ • Steam News API      │
                    │ • Reddit API          │
                    │ • SQLite/Supabase DB  │
                    └───────────────────────┘
```

### Agent Specifications

| Agent | Function | Input | Output |
|-------|----------|-------|--------|
| **Planner** | Selects relevant analysts based on market context | Ticker, available analysts | List of selected analysts |
| **Technical Analyst** | Analyzes price patterns and trends | Historical price data | Technical signals (BUY/SELL/HOLD) |
| **Sentiment Analyst** | Processes community sentiment | Reddit posts, Steam news | Sentiment score and direction |
| **Liquidity Analyst** | Evaluates market depth and volume | Order book, trading volume | Liquidity assessment |
| **Event Analyst** | Identifies market-moving events | News, updates | Event impact analysis |
| **Portfolio Manager** | Executes risk-aware trading decisions | Analyst signals, portfolio state | Trading actions with position sizes |

---

## ✨ Key Features

### 🤖 Multi-Modal Analysis
- **Technical Analysis**: Price action, trend detection, support/resistance levels
- **Sentiment Analysis**: NLP-based sentiment extraction from Reddit and Steam communities
- **Liquidity Analysis**: Market depth and volume-based assessments
- **Event-Driven Analysis**: Impact evaluation of game updates and news

### 🧠 Intelligent Agent Coordination
- **Meta-Planner**: Dynamically selects optimal analyst combination for each ticker
- **Modular Design**: Easily extensible agent registry system
- **Flexible Workflows**: Support for both agentic and direct LLM modes

### 💼 Advanced Portfolio Management
- **Risk Control**: Maximum position ratio constraints and drawdown protection
- **Transaction Costs**: Realistic modeling of 2% trading fees
- **Position Sizing**: Intelligent allocation across multiple assets
- **State Persistence**: Complete portfolio history tracking in database

### 🔧 Production-Ready Infrastructure
- **Database Support**: SQLite for local development, Supabase for production
- **Multi-Provider LLM**: OpenAI, Anthropic, DeepSeek, Ollama, etc.
- **Extensive Configuration**: 50+ pre-configured experiment setups
- **Comprehensive Logging**: Detailed agent execution and decision tracking

---

## 📦 Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager
- (Optional) SQLite for local database
- (Optional) Supabase account for cloud database

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/IatomicreactorI/CSGOTrading.git
cd CSGOTrading
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env and add your API keys:
# - OPENAI_API_KEY (for OpenAI models)
# - ANTHROPIC_API_KEY (for Claude models)
# - DEEPSEEK_API_KEY (for DeepSeek models)
# - SUPABASE_URL and SUPABASE_KEY (for cloud database)
```

5. **Initialize database** (if using local SQLite)
```bash
python src/database/cs2_sqlite_setup.py
```

---

## 🚀 Quick Start

### Basic Usage

Run a single-day experiment with default configuration:

```bash
python src/run.py --config TS-ds.yaml --start-date 2025-09-25 --end-date 2025-09-25
```

### Batch Experiments

Run multi-day backtesting:

```bash
python src/run.py \
  --config TS-ds.yaml \
  --start-date 2025-09-25 \
  --end-date 2025-10-27
```

### Configuration Options

The system supports multiple workflow configurations:

- **Direct**: Direct LLM analysis without analyst agents
- **T**: Technical analyst only
- **TS**: Technical + Sentiment analysts
- **TSL**: Technical + Sentiment + Liquidity
- **TSLE**: All analysts (Technical + Sentiment + Liquidity + Event)
- **TSrL**: Technical + Reverse Sentiment + Liquidity

Each configuration can be combined with different LLM providers:
- `-ds`: DeepSeek
- `-gm`: Gemini
- `-gt`: GPT
- `-cd`: Claude
- `-km`: Kimi
- `-qw`: Qwen

Example: `TSLE-cd.yaml` uses all analysts with Claude 3.5 Sonnet.

### View Results

```bash
# View portfolio performance
python src/view.py --config-name TS-ds --date 2025-09-26

# Clear experiment data
python src/clear.py --config-name TS-ds
```

---

## 📊 Configuration

### Workflow Configuration (`src/config/`)

```yaml
exp_name: "TS-ds"  # Experiment name
cashflow: 10000    # Initial capital
tickers:           # Assets to trade
  - "AK-47 | Redline (Field-Tested)"
  - "AWP | Asiimov (Field-Tested)"

llm:               # LLM configuration
  provider: "deepseek"
  model: "deepseek-chat"
  temperature: 0.5

planner_mode: true        # Enable meta-planner
workflow_analysts:        # Available analysts
  - technical
  - sentiment

enable_transaction_fee: true  # Include trading costs
```

### Database Configuration

**Local SQLite** (default):
```bash
python src/run.py --config TS-ds.yaml
```

**Supabase Cloud**:
```bash
python src/run.py --config TS-ds.yaml --no-local-db
```

---


## 🗂️ Project Structure

```
CSGOTrading/
├── src/
│   ├── agents/              # Agent implementations
│   │   ├── planner.py       # Meta-planner agent
│   │   ├── portfolio_manager.py  # Portfolio management
│   │   ├── registry.py      # Agent registry
│   │   └── analysts/        # Specialized analysts
│   │       ├── technical.py
│   │       ├── sentiment.py
│   │       ├── liquidity.py
│   │       └── event.py
│   ├── apis/                # Data source integrations
│   │   ├── cs2market/       # CS2 market data
│   │   ├── steam/           # Steam news API
│   │   └── reddit/          # Reddit sentiment API
│   ├── database/            # Database layer
│   │   ├── interface.py     # Abstract interface
│   │   └── cs2_sqlite_helper.py
│   ├── graph/               # LangGraph workflow
│   │   ├── workflow.py      # Main workflow
│   │   ├── schema.py        # State definitions
│   │   └── constants.py
│   ├── llm/                 # LLM integration
│   │   ├── inference.py     # LLM calls
│   │   ├── provider.py      # Provider configs
│   │   └── prompt.py        # Prompt templates
│   ├── config/              # Experiment configurations
│   ├── util/                # Utilities
│   ├── run.py               # Main execution script
│   ├── view.py              # Results visualization
│   └── clear.py             # Data cleanup
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🔬 Advanced Usage

### Adding Custom Analysts

1. Create analyst implementation in `src/agents/analysts/`:

```python
from graph.constants import AgentKey
from llm.inference import agent_call

def custom_analyst(ticker: str, llm_config, analyst_signal):
    # Your analysis logic here
    return {
        "action": "BUY",
        "confidence": 0.8,
        "justification": "Analysis reasoning"
    }
```

2. Register in `src/agents/registry.py`:

```python
AgentRegistry.register(
    AgentKey.CUSTOM,
    custom_analyst,
    "Custom analyst description"
)
```

3. Add to workflow configuration:

```yaml
workflow_analysts:
  - technical
  - sentiment
  - custom  # Your new analyst
```

### Custom LLM Providers

Add provider configuration in `src/llm/provider.py`:

```python
@dataclass
class ProviderConfig:
    name: str
    model_class: Any
    requires_api_key: bool = True
    env_key: str = "CUSTOM_API_KEY"
    base_url: str = None

# Register provider
Provider.add_provider("custom", ProviderConfig(...))
```

---

## 📚 Documentation

### API Reference

Detailed API documentation is available in the `docs/` directory:
- [Agent Development Guide](docs/agents.md)
- [Database Schema](docs/database.md)
- [Configuration Reference](docs/config.md)
- [LLM Integration](docs/llm.md)

### Workflow Explanation

For a comprehensive understanding of the system workflow, see [WORKFLOW_EXPLANATION.md](src/WORKFLOW_EXPLANATION.md).

---

## 🤝 Contributing

We welcome contributions from the community! Please follow these guidelines:

1. **Fork the repository** and create a feature branch
2. **Follow PEP 8** coding standards
3. **Add tests** for new functionality
4. **Update documentation** for API changes
5. **Submit a pull request** with a clear description

### Development Setup

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Run linting
flake8 src/
black src/
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **LangChain & LangGraph**: Foundation for agent orchestration
- **OpenAI, Anthropic, DeepSeek**: LLM providers
- **CS2 Community**: Market data and insights
- **Contributors**: All community contributors

---

## 🗺️ Roadmap

### Upcoming Features

- [ ] **Reinforcement Learning Integration**: Train agents with RL for adaptive strategies
- [ ] **Real-time Trading**: Live market integration and execution
- [ ] **Advanced Risk Models**: VaR, CVaR, and Kelly criterion
- [ ] **Multi-Asset Correlation**: Cross-asset analysis and hedging
- [ ] **Web Dashboard**: Interactive visualization and monitoring
- [ ] **Distributed Execution**: Multi-process and cloud deployment
- [ ] **More Data Sources**: Integration with additional market APIs

### Version History

- **v0.1.0** (2026-01-05): Initial release with core multi-agent framework
- **v0.0.1** (2025-12): Private beta testing

---

<div align="center">

**⭐ Star this repository if you find it useful! ⭐**

Made with ❤️ by the CSGOTrading Team

</div>
