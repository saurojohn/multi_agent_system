"""AI Agent API integration module."""

import os
import json
import logging
import time
import asyncio
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger('ai_agent')

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


class AIProvider(Enum):
    """AI provider types."""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    OLLAMA = "ollama"
    MINIMAX = "minimax"
    DEEPSEEK = "deepseek"
    ZHIPU = "zhipu"
    CUSTOM = "custom"


@dataclass
class AIConfig:
    """AI configuration."""
    provider: AIProvider = AIProvider.OPENAI
    api_key: str = ""
    model: str = "gpt-4"
    base_url: str = ""
    max_tokens: int = 4096
    temperature: float = 0.7
    timeout: int = 60


class AIResponse:
    """AI API response wrapper."""
    def __init__(self, content: str, provider: str, model: str, usage: Dict = None, raw: Any = None):
        self.content = content
        self.provider = provider
        self.model = model
        self.usage = usage or {}
        self.raw = raw
        self.success = True
        self.error: Optional[str] = None


class AIAgent:
    """
    AI Agent that wraps AI API calls.
    Can be used as a handler in WorkerAgent.
    """

    def __init__(self, config: AIConfig = None):
        self.config = config or AIConfig()
        self._client = None
        self._init_client()

    def _init_client(self):
        """Initialize the AI client based on provider."""
        if self.config.provider == AIProvider.OPENAI and OPENAI_AVAILABLE:
            if self.config.api_key:
                openai.api_key = self.config.api_key
            if self.config.base_url:
                openai.base_url = self.config.base_url
            self._client = openai.OpenAI(
                api_key=self.config.api_key or os.environ.get("OPENAI_API_KEY"),
                base_url=self.config.base_url or os.environ.get("OPENAI_BASE_URL")
            )

        elif self.config.provider == AIProvider.ANTHROPIC and ANTHROPIC_AVAILABLE:
            self._client = anthropic.Anthropic(
                api_key=self.config.api_key or os.environ.get("ANTHROPIC_API_KEY")
            )

        elif self.config.provider == AIProvider.OLLAMA:
            self._base_url = self.config.base_url or "http://localhost:11434"
            self._client = "ollama"

        elif self.config.provider == AIProvider.CUSTOM:
            # Custom OpenAI-compatible API
            if not OPENAI_AVAILABLE:
                self._client = None
            else:
                self._client = openai.OpenAI(
                    api_key=self.config.api_key or "dummy",
                    base_url=self.config.base_url or "http://localhost:8000/v1"
                )

        elif self.config.provider == AIProvider.MINIMAX:
            # Minimax AI (uses OpenAI-compatible API)
            if not OPENAI_AVAILABLE:
                self._client = None
            else:
                self._client = openai.OpenAI(
                    api_key=self.config.api_key,
                    base_url=self.config.base_url or "https://api.minimax.chat/v1"
                )

        elif self.config.provider == AIProvider.DEEPSEEK:
            # DeepSeek AI
            if not OPENAI_AVAILABLE:
                self._client = None
            else:
                self._client = openai.OpenAI(
                    api_key=self.config.api_key,
                    base_url=self.config.base_url or "https://api.deepseek.com/v1"
                )

        elif self.config.provider == AIProvider.ZHIPU:
            # Zhipu AI (智谱AI)
            if not OPENAI_AVAILABLE:
                self._client = None
            else:
                self._client = openai.OpenAI(
                    api_key=self.config.api_key,
                    base_url=self.config.base_url or "https://open.bigmodel.cn/api/paas/v4"
                )

    def chat(self, messages: list, system: str = None, **kwargs) -> AIResponse:
        """
        Send chat request to AI.

        Args:
            messages: List of message dicts with 'role' and 'content'
            system: Optional system prompt
            **kwargs: Additional provider-specific args

        Returns:
            AIResponse object
        """
        try:
            if self.config.provider == AIProvider.OPENAI:
                return self._chat_openai(messages, system, **kwargs)
            elif self.config.provider == AIProvider.ANTHROPIC:
                return self._chat_anthropic(messages, system, **kwargs)
            elif self.config.provider == AIProvider.OLLAMA:
                return self._chat_ollama(messages, system, **kwargs)
            elif self.config.provider in (AIProvider.CUSTOM, AIProvider.MINIMAX, AIProvider.DEEPSEEK, AIProvider.ZHIPU):
                return self._chat_openai(messages, system, **kwargs)
            else:
                return AIResponse("", "unknown", self.config.model, success=False, error="Unknown provider")
        except Exception as e:
            logger.error(f"AI chat error: {e}")
            return AIResponse("", self.config.provider.value, self.config.model, success=False, error=str(e))

    def _chat_openai(self, messages: list, system: str = None, **kwargs) -> AIResponse:
        """OpenAI chat completion."""
        if not OPENAI_AVAILABLE:
            return AIResponse("", "openai", self.config.model, success=False, error="OpenAI not installed")

        full_messages = []
        if system:
            full_messages.append({"role": "system", "content": system})
        full_messages.extend(messages)

        response = self._client.chat.completions.create(
            model=self.config.model,
            messages=full_messages,
            max_tokens=kwargs.get("max_tokens", self.config.max_tokens),
            temperature=kwargs.get("temperature", self.config.temperature),
            timeout=kwargs.get("timeout", self.config.timeout)
        )

        content = response.choices[0].message.content
        usage = {
            "prompt_tokens": response.usage.prompt_tokens if hasattr(response.usage, 'prompt_tokens') else 0,
            "completion_tokens": response.usage.completion_tokens if hasattr(response.usage, 'completion_tokens') else 0,
            "total_tokens": response.usage.total_tokens if hasattr(response.usage, 'total_tokens') else 0
        }
        return AIResponse(content, "openai", self.config.model, usage, response)

    def _chat_anthropic(self, messages: list, system: str = None, **kwargs) -> AIResponse:
        """Anthropic Claude chat."""
        if not ANTHROPIC_AVAILABLE:
            return AIResponse("", "anthropic", self.config.model, success=False, error="Anthropic not installed")

        response = self._client.messages.create(
            model=self.config.model,
            max_tokens=kwargs.get("max_tokens", self.config.max_tokens),
            temperature=kwargs.get("temperature", self.config.temperature),
            system=system,
            messages=messages
        )

        content = response.content[0].text
        usage = {
            "input_tokens": response.usage.input_tokens if hasattr(response.usage, 'input_tokens') else 0,
            "output_tokens": response.usage.output_tokens if hasattr(response.usage, 'output_tokens') else 0
        }
        return AIResponse(content, "anthropic", self.config.model, usage, response)

    def _chat_ollama(self, messages: list, system: str = None, **kwargs) -> AIResponse:
        """Ollama local LLM chat."""
        import requests

        full_prompt = ""
        if system:
            full_prompt += f"System: {system}\n"

        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            full_prompt += f"{role}: {content}\n"

        try:
            response = requests.post(
                f"{self._base_url}/api/generate",
                json={
                    "model": self.config.model,
                    "prompt": full_prompt,
                    "stream": False
                },
                timeout=kwargs.get("timeout", self.config.timeout)
            )
            response.raise_for_status()
            data = response.json()
            return AIResponse(data.get("response", ""), "ollama", self.config.model)
        except Exception as e:
            return AIResponse("", "ollama", self.config.model, success=False, error=str(e))

    def complete(self, prompt: str, **kwargs) -> AIResponse:
        """Simple prompt completion."""
        return self.chat([{"role": "user", "content": prompt}], **kwargs)

    def analyze(self, data: str, task: str = "分析", **kwargs) -> AIResponse:
        """
        Analyze data with AI.

        Args:
            data: Data to analyze
            task: Analysis task description
            **kwargs: Additional arguments

        Returns:
            AIResponse with analysis result
        """
        system_prompt = f"""你是一个专业的AI数据分析助手。
当给定数据时，你需要：
1. 理解数据结构和内容
2. 提取关键信息和模式
3. 提供有价值的洞察和建议
4. 如果需要，可以进行计算、统计、分类等操作

任务类型：{task}
"""
        return self.chat([{"role": "user", "content": f"请分析以下数据：\n{data}"}], system=system_prompt, **kwargs)

    def research(self, topic: str, depth: str = "中等", **kwargs) -> AIResponse:
        """
        Research a topic.

        Args:
            topic: Research topic
            depth: Research depth (简短/中等/深入)
            **kwargs: Additional arguments

        Returns:
            AIResponse with research results
        """
        depth_instruction = {
            "简短": "用简洁的语言给出2-3个要点的总结",
            "中等": "提供中等详细程度的分析，包括背景、关键点和结论",
            "深入": "进行深入全面的研究分析，包括背景、现状、趋势、挑战、机会和详细建议"
        }.get(depth, "提供中等详细的分析")

        system_prompt = f"""你是一个专业的研究助手。
请对给定主题进行{depth}程度的研究分析。
{depth_instruction}

请用结构化的方式组织回答。"""
        return self.chat([{"role": "user", "content": f"研究主题：{topic}"}], system=system_prompt, **kwargs)

    def code(self, requirement: str, language: str = "python", **kwargs) -> AIResponse:
        """
        Generate code.

        Args:
            requirement: Code requirement
            language: Programming language
            **kwargs: Additional arguments

        Returns:
            AIResponse with generated code
        """
        system_prompt = f"""你是一个专业的代码助手。
根据用户需求生成高质量的{language}代码。

要求：
1. 代码应该完整、可运行
2. 包含必要的注释
3. 遵循最佳实践
4. 适当处理错误情况"""
        return self.chat([{"role": "user", "content": f"需求：{requirement}"}], system=system_prompt, **kwargs)

    def design(self, requirement: str, design_type: str = "general", **kwargs) -> AIResponse:
        """
        Design output.

        Args:
            requirement: Design requirement
            design_type: Type of design (ui/ux/architecture/general)
            **kwargs: Additional arguments

        Returns:
            AIResponse with design suggestions
        """
        type_instructions = {
            "ui": "你是一个UI设计专家。请提供界面设计建议，包括布局、配色、组件等。",
            "ux": "你是一个UX设计专家。请提供用户体验优化建议，包括交互流程、信息架构等。",
            "architecture": "你是一个架构设计专家。请提供系统架构设计建议，包括模块划分、技术选型等。",
            "general": "你是一个设计专家。请提供综合设计建议。"
        }.get(design_type, "你是一个设计专家。")

        system_prompt = f"""{type_instructions}
请给出详细、可行的设计方案。"""
        return self.chat([{"role": "user", "content": f"设计需求：{requirement}"}], system=system_prompt, **kwargs)


def create_ai_handler(config: AIConfig) -> Callable:
    """
    Create a handler function for WorkerAgent that uses AI.

    Usage:
        from multi_agent_system.worker.agent import WorkerAgent
        from multi_agent_system.common.ai_agent import create_ai_handler, AIConfig, AIProvider

        config = AIConfig(provider=AIProvider.OPENAI, model="gpt-4", api_key="sk-...")
        handler = create_ai_handler(config)

        worker = WorkerAgent(worker_id="ai_worker", worker_type="AI", capabilities=["analysis"], mq=mq)
        worker.register_handler("analysis", handler)
        worker.start()
    """
    agent = AIAgent(config)

    def handler(task_data: Dict) -> Dict:
        task_type = task_data.get("task_type", "analysis")
        data = task_data.get("task_data", task_data)

        if task_type == "analysis":
            content = data.get("query", "") or data.get("data", "")
            result = agent.analyze(content, task=data.get("task", "分析"))
        elif task_type == "research":
            topic = data.get("topic", "") or data.get("query", "")
            result = agent.research(topic, depth=data.get("depth", "中等"))
        elif task_type == "coding":
            requirement = data.get("requirement", "") or data.get("code", "")
            language = data.get("language", "python")
            result = agent.code(requirement, language)
        elif task_type == "design":
            requirement = data.get("requirement", "") or data.get("spec", "")
            design_type = data.get("design_type", "general")
            result = agent.design(requirement, design_type)
        else:
            prompt = str(data)
            result = agent.complete(prompt)

        if result.success:
            return {
                "result": result.content,
                "provider": result.provider,
                "model": result.model,
                "usage": result.usage
            }
        else:
            return {
                "error": result.error,
                "provider": result.provider,
                "model": result.model
            }

    return handler


# Global default AI agent
_default_agent: Optional[AIAgent] = None


def get_default_agent() -> AIAgent:
    """Get or create default AI agent."""
    global _default_agent
    if _default_agent is None:
        _default_agent = AIAgent()
    return _default_agent


def configure_default_agent(config: AIConfig):
    """Configure the default AI agent."""
    global _default_agent
    _default_agent = AIAgent(config)


class AIChatSession:
    """Multi-turn chat session with AI."""

    def __init__(self, agent: AIAgent = None, system: str = None):
        self.agent = agent or get_default_agent()
        self.system = system
        self.messages = []
        self.created_at = time.time()

    def add_user_message(self, content: str):
        """Add user message to history."""
        self.messages.append({"role": "user", "content": content})

    def send(self, message: str = None) -> AIResponse:
        """
        Send message to AI and get response.

        Args:
            message: Optional new message to add before sending

        Returns:
            AIResponse from AI
        """
        if message:
            self.add_user_message(message)

        response = self.agent.chat(self.messages, system=self.system)
        if response.success and response.content:
            self.messages.append({"role": "assistant", "content": response.content})

        return response

    def clear(self):
        """Clear conversation history."""
        self.messages = []

    def get_history(self) -> list:
        """Get conversation history."""
        return list(self.messages)


# Convenience functions
def quick_chat(prompt: str, provider: str = "openai", model: str = "gpt-4", **kwargs) -> str:
    """
    Quick chat with AI without creating objects.

    Usage:
        result = quick_chat("What is 2+2?", provider="openai", model="gpt-4")
    """
    config = AIConfig(provider=AIProvider(provider), model=model, **kwargs)
    agent = AIAgent(config)
    response = agent.complete(prompt)
    return response.content if response.success else f"Error: {response.error}"


def quick_analyze(data: str, task: str = "分析", **kwargs) -> str:
    """Quick data analysis with AI."""
    agent = get_default_agent()
    result = agent.analyze(data, task, **kwargs)
    return result.content if result.success else f"Error: {result.error}"


def quick_code(requirement: str, language: str = "python", **kwargs) -> str:
    """Quick code generation with AI."""
    agent = get_default_agent()
    result = agent.code(requirement, language, **kwargs)
    return result.content if result.success else f"Error: {result.error}"