"""
Fallback LLM Client - Multi-provider support with automatic fallback
=====================================================================

Uses auth_mcp to get model configurations and API keys, then tries
each provider in order until one succeeds.

Supports: Anthropic, OpenAI, Google Gemini, xAI Grok

This is the authoritative location for FallbackLLMClient.
Other projects should import from here:
    from auth_mcp.fallback_client import FallbackLLMClient
"""

import os
import json
import hashlib
import secrets
import logging
from typing import Optional, Dict, Any, List, Generator
from dataclasses import dataclass
import requests

logger = logging.getLogger(__name__)

# Auth MCP configuration
AUTH_MCP_URL = os.environ.get("AUTH_MCP_URL", "https://auth-mcp.urbancanary.workers.dev")


def generate_auth_token() -> str:
    """Generate a self-validating token for auth_mcp."""
    random_part = secrets.token_hex(8)
    checksum = hashlib.sha256(random_part.encode()).hexdigest()[:8]
    return f"{random_part}-{checksum}"


@dataclass
class ModelConfig:
    """Configuration for an LLM model."""
    name: str
    model_id: str
    provider: str
    api_key_name: str
    priority: int = 1


@dataclass
class FallbackResponse:
    """Response from the fallback client."""
    text: str
    model_used: str
    provider: str
    success: bool
    error: Optional[str] = None


class FallbackLLMClient:
    """
    Multi-provider LLM client with automatic fallback.

    Uses auth_mcp to:
    1. Get ordered model list for a purpose (cheapest first)
    2. Get API keys for each provider
    3. Try each provider until one succeeds

    Available purposes (from auth_mcp/worker.js):
    - routing: Query routing to agents (cheapest first)
    - parsing: Document/response parsing
    - agents: Agent conversations
    - analysis: Deep analysis tasks
    - deep_research: Complex research
    - memory: Memory operations (cost-sensitive)
    - chat: General chat (benefits from caching)
    """

    def __init__(self, purpose: str = "agents", requester: str = "fallback-client"):
        """
        Initialize client for a specific purpose.

        Args:
            purpose: The use case - 'agents', 'routing', 'analysis', etc.
            requester: Identifier for the calling service (for logging)
        """
        self.purpose = purpose
        self.requester = requester
        self.auth_url = AUTH_MCP_URL
        self._models: List[ModelConfig] = []
        self._api_keys: Dict[str, str] = {}
        self._initialized = False

    def _get_auth_headers(self) -> Dict[str, str]:
        """Get headers for auth_mcp requests."""
        return {
            "Authorization": f"Bearer {generate_auth_token()}",
            "X-Requester": self.requester,
            "Content-Type": "application/json"
        }

    def _fetch_models_for_purpose(self) -> List[ModelConfig]:
        """Fetch model configuration from auth_mcp for this purpose."""
        try:
            response = requests.get(
                f"{self.auth_url}/api/purpose/{self.purpose}",
                headers=self._get_auth_headers(),
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                models = []

                for model_data in data.get("all_models", []):
                    models.append(ModelConfig(
                        name=model_data["name"],
                        model_id=model_data["model_id"],
                        provider=model_data["provider"],
                        api_key_name=model_data["api_key"],
                        priority=model_data.get("priority", 1)
                    ))

                logger.info(f"Loaded {len(models)} models for purpose '{self.purpose}'")
                return models
            else:
                logger.warning(f"Failed to fetch models: {response.status_code}")
                return self._default_models()

        except Exception as e:
            logger.error(f"Error fetching models from auth_mcp: {e}")
            return self._default_models()

    def _default_models(self) -> List[ModelConfig]:
        """Default model configuration if auth_mcp is unavailable."""
        # Purpose-aware defaults - aligned with auth_mcp/worker.js
        if self.purpose == "routing":
            # Routing: cheapest first (Gemini Flash is $0.075/1M)
            return [
                ModelConfig("GEMINI_FLASH", "gemini-2.0-flash-lite", "google", "GEMINI_API_KEY", 1),
                ModelConfig("OPENAI_MINI", "gpt-4o-mini", "openai", "OPENAI_API_KEY", 2),
                ModelConfig("CLAUDE_HAIKU", "claude-haiku-4-5", "anthropic", "ANTHROPIC_API_KEY", 3),
            ]
        elif self.purpose == "chat":
            # Chat benefits from Gemini's context caching
            return [
                ModelConfig("GEMINI_FLASH", "gemini-2.0-flash-lite", "google", "GEMINI_API_KEY", 1),
                ModelConfig("CLAUDE_HAIKU", "claude-haiku-4-5", "anthropic", "ANTHROPIC_API_KEY", 2),
                ModelConfig("OPENAI_MINI", "gpt-4o-mini", "openai", "OPENAI_API_KEY", 3),
            ]
        elif self.purpose == "memory":
            # Memory: cost-sensitive
            return [
                ModelConfig("GEMINI_FLASH", "gemini-2.0-flash-lite", "google", "GEMINI_API_KEY", 1),
                ModelConfig("OPENAI_MINI", "gpt-4o-mini", "openai", "OPENAI_API_KEY", 2),
                ModelConfig("CLAUDE_HAIKU", "claude-haiku-4-5", "anthropic", "ANTHROPIC_API_KEY", 3),
            ]
        # Default order for other purposes (agents, parsing, etc.)
        return [
            ModelConfig("CLAUDE_HAIKU", "claude-haiku-4-5", "anthropic", "ANTHROPIC_API_KEY", 1),
            ModelConfig("OPENAI_MINI", "gpt-4o-mini", "openai", "OPENAI_API_KEY", 2),
            ModelConfig("GEMINI_FLASH", "gemini-2.0-flash-lite", "google", "GEMINI_API_KEY", 3),
        ]

    def _fetch_api_key(self, key_name: str) -> Optional[str]:
        """Fetch an API key from auth_mcp (primary) or environment (fallback)."""
        # Check cache first
        if key_name in self._api_keys:
            return self._api_keys[key_name]

        # Try auth_mcp first (authoritative source)
        try:
            response = requests.get(
                f"{self.auth_url}/api/key/{key_name}",
                headers=self._get_auth_headers(),
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                value = data.get("value")
                if value:
                    self._api_keys[key_name] = value
                    return value
        except Exception as e:
            logger.warning(f"auth_mcp fetch failed for {key_name}: {e}")

        # Fallback to environment variable
        env_value = os.environ.get(key_name)
        if env_value:
            logger.debug(f"Using {key_name} from environment (auth_mcp unavailable)")
            self._api_keys[key_name] = env_value
            return env_value

        return None

    def initialize(self):
        """Load models and prepare for requests."""
        if not self._initialized:
            self._models = self._fetch_models_for_purpose()
            self._initialized = True

    def _call_anthropic(self, model_id: str, api_key: str, messages: List[Dict],
                        system: str, max_tokens: int) -> FallbackResponse:
        """Call Anthropic API."""
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)

            response = client.messages.create(
                model=model_id,
                max_tokens=max_tokens,
                system=system,
                messages=messages
            )

            text = ""
            for block in response.content:
                if hasattr(block, 'text'):
                    text += block.text

            return FallbackResponse(
                text=text,
                model_used=model_id,
                provider="anthropic",
                success=True
            )

        except Exception as e:
            error_msg = str(e)
            if "credit balance" in error_msg.lower() or "rate limit" in error_msg.lower():
                logger.warning(f"Anthropic API failed (will try fallback): {error_msg[:100]}")
            else:
                logger.error(f"Anthropic API error: {error_msg}")
            return FallbackResponse(
                text="",
                model_used=model_id,
                provider="anthropic",
                success=False,
                error=error_msg
            )

    def _call_openai(self, model_id: str, api_key: str, messages: List[Dict],
                     system: str, max_tokens: int) -> FallbackResponse:
        """Call OpenAI API."""
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key)

            # Prepend system message for OpenAI format
            openai_messages = [{"role": "system", "content": system}]
            openai_messages.extend(messages)

            response = client.chat.completions.create(
                model=model_id,
                max_tokens=max_tokens,
                messages=openai_messages
            )

            text = response.choices[0].message.content or ""

            return FallbackResponse(
                text=text,
                model_used=model_id,
                provider="openai",
                success=True
            )

        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return FallbackResponse(
                text="",
                model_used=model_id,
                provider="openai",
                success=False,
                error=str(e)
            )

    def _call_google(self, model_id: str, api_key: str, messages: List[Dict],
                     system: str, max_tokens: int) -> FallbackResponse:
        """Call Google Gemini API using new google.genai SDK."""
        try:
            from google import genai
            from google.genai import types

            client = genai.Client(api_key=api_key)

            # Build content from messages
            # For chat, we combine all messages into a conversation
            contents = []
            for msg in messages:
                role = "user" if msg["role"] == "user" else "model"
                contents.append(types.Content(role=role, parts=[types.Part(text=msg["content"])]))

            response = client.models.generate_content(
                model=model_id,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=system,
                    max_output_tokens=max_tokens
                )
            )

            text = response.text or ""

            return FallbackResponse(
                text=text,
                model_used=model_id,
                provider="google",
                success=True
            )

        except Exception as e:
            logger.error(f"Google Gemini API error: {e}")
            return FallbackResponse(
                text="",
                model_used=model_id,
                provider="google",
                success=False,
                error=str(e)
            )

    def _call_xai(self, model_id: str, api_key: str, messages: List[Dict],
                  system: str, max_tokens: int) -> FallbackResponse:
        """Call xAI Grok API (OpenAI-compatible)."""
        try:
            from openai import OpenAI
            client = OpenAI(
                api_key=api_key,
                base_url="https://api.x.ai/v1"
            )

            xai_messages = [{"role": "system", "content": system}]
            xai_messages.extend(messages)

            response = client.chat.completions.create(
                model=model_id,
                max_tokens=max_tokens,
                messages=xai_messages
            )

            text = response.choices[0].message.content or ""

            return FallbackResponse(
                text=text,
                model_used=model_id,
                provider="xai",
                success=True
            )

        except Exception as e:
            logger.error(f"xAI Grok API error: {e}")
            return FallbackResponse(
                text="",
                model_used=model_id,
                provider="xai",
                success=False,
                error=str(e)
            )

    def chat(
        self,
        messages: List[Dict],
        system: str = "You are a helpful assistant.",
        max_tokens: int = 4096
    ) -> FallbackResponse:
        """
        Send a chat request, trying each provider until one succeeds.

        Args:
            messages: List of {"role": "user"|"assistant", "content": "..."}
            system: System prompt
            max_tokens: Maximum tokens in response

        Returns:
            FallbackResponse with text and metadata
        """
        self.initialize()

        errors = []

        for model in self._models:
            api_key = self._fetch_api_key(model.api_key_name)

            if not api_key:
                logger.warning(f"No API key available for {model.provider}")
                errors.append(f"{model.provider}: No API key")
                continue

            logger.info(f"Trying {model.provider}/{model.model_id}...")

            if model.provider == "anthropic":
                result = self._call_anthropic(model.model_id, api_key, messages, system, max_tokens)
            elif model.provider == "openai":
                result = self._call_openai(model.model_id, api_key, messages, system, max_tokens)
            elif model.provider == "google":
                result = self._call_google(model.model_id, api_key, messages, system, max_tokens)
            elif model.provider == "xai":
                result = self._call_xai(model.model_id, api_key, messages, system, max_tokens)
            else:
                logger.warning(f"Unknown provider: {model.provider}")
                continue

            if result.success:
                logger.info(f"Success with {model.provider}/{model.model_id}")
                return result
            else:
                errors.append(f"{model.provider}: {result.error}")

        # All providers failed
        return FallbackResponse(
            text=f"All providers failed. Errors: {'; '.join(errors)}",
            model_used="none",
            provider="none",
            success=False,
            error="; ".join(errors)
        )

    def stream(
        self,
        messages: List[Dict],
        system: str = "You are a helpful assistant.",
        max_tokens: int = 4096
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Stream a chat response, trying each provider until one succeeds.

        Yields events: {"type": "text"|"error"|"done", "content": "...", ...}
        """
        self.initialize()

        for model in self._models:
            api_key = self._fetch_api_key(model.api_key_name)

            if not api_key:
                continue

            logger.info(f"Trying stream with {model.provider}/{model.model_id}...")

            try:
                if model.provider == "anthropic":
                    yield from self._stream_anthropic(model.model_id, api_key, messages, system, max_tokens)
                    return
                elif model.provider == "openai":
                    yield from self._stream_openai(model.model_id, api_key, messages, system, max_tokens)
                    return
                elif model.provider == "google":
                    yield from self._stream_google(model.model_id, api_key, messages, system, max_tokens)
                    return
                else:
                    # Non-streaming fallback for other providers
                    result = self.chat(messages, system, max_tokens)
                    if result.success:
                        yield {"type": "text", "content": result.text}
                        yield {"type": "done", "model": result.model_used, "provider": result.provider}
                        return
            except Exception as e:
                logger.warning(f"Stream failed for {model.provider}: {e}")
                continue

        yield {"type": "error", "content": "All providers failed"}

    def _stream_anthropic(self, model_id: str, api_key: str, messages: List[Dict],
                          system: str, max_tokens: int) -> Generator[Dict, None, None]:
        """Stream from Anthropic."""
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        with client.messages.stream(
            model=model_id,
            max_tokens=max_tokens,
            system=system,
            messages=messages
        ) as stream:
            for text in stream.text_stream:
                yield {"type": "text", "content": text}

        yield {"type": "done", "model": model_id, "provider": "anthropic"}

    def _stream_openai(self, model_id: str, api_key: str, messages: List[Dict],
                       system: str, max_tokens: int) -> Generator[Dict, None, None]:
        """Stream from OpenAI."""
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        openai_messages = [{"role": "system", "content": system}]
        openai_messages.extend(messages)

        stream = client.chat.completions.create(
            model=model_id,
            max_tokens=max_tokens,
            messages=openai_messages,
            stream=True
        )

        for chunk in stream:
            if chunk.choices[0].delta.content:
                yield {"type": "text", "content": chunk.choices[0].delta.content}

        yield {"type": "done", "model": model_id, "provider": "openai"}

    def _stream_google(self, model_id: str, api_key: str, messages: List[Dict],
                       system: str, max_tokens: int) -> Generator[Dict, None, None]:
        """Stream from Google Gemini using new google.genai SDK."""
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)

        # Build content from messages
        contents = []
        for msg in messages:
            role = "user" if msg["role"] == "user" else "model"
            contents.append(types.Content(role=role, parts=[types.Part(text=msg["content"])]))

        response = client.models.generate_content_stream(
            model=model_id,
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=system,
                max_output_tokens=max_tokens
            )
        )

        for chunk in response:
            if chunk.text:
                yield {"type": "text", "content": chunk.text}

        yield {"type": "done", "model": model_id, "provider": "google"}


# Convenience function
def get_fallback_client(purpose: str = "agents", requester: str = "fallback-client") -> FallbackLLMClient:
    """Get a fallback client for the specified purpose."""
    return FallbackLLMClient(purpose=purpose, requester=requester)
