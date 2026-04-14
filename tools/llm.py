from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


class LLMError(RuntimeError):
    pass


@dataclass
class LLMConfig:
    provider: str
    api_key: str
    model: str
    base_url: str
    timeout: int = 180
    temperature: float = 0.2
    max_tokens: int = 8192

    def __post_init__(self) -> None:
        self.provider = self.provider.lower().strip()
        self.base_url = self.base_url.rstrip("/")

        if self.provider not in {"openai", "anthropic"}:
            raise ValueError("provider must be 'openai' or 'anthropic'")


@dataclass
class LLMResult:
    provider: str
    model: str
    text: str
    raw: dict[str, Any]
    usage: dict[str, Any]


class LLMClient:
    def __init__(self, config: LLMConfig) -> None:
        self.config = config

    def generate(
        self,
        *,
        system: str | None = None,
        prompt: str | None = None,
        messages: list[dict[str, str]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> LLMResult:
        normalized_messages = self._normalize_messages(system, prompt, messages)

        if self.config.provider == "openai":
            return self._call_openai(normalized_messages, response_format=response_format)

        return self._call_anthropic(normalized_messages)

    def generate_json(
        self,
        *,
        system: str | None = None,
        prompt: str | None = None,
        messages: list[dict[str, str]] | None = None,
    ) -> tuple[dict[str, Any], LLMResult]:
        guidance = (
            "Return valid JSON only. Do not wrap it in markdown fences or add extra text."
        )
        final_system = f"{system}\n\n{guidance}" if system else guidance
        result = self.generate(system=final_system, prompt=prompt, messages=messages)

        from utils.json_utils import parse_llm_json
        
        try:
            parsed = parse_llm_json(result.text)
            return parsed, result
        except Exception as exc:
            if isinstance(exc, LLMError):
                raise
            raise LLMError(f"Model did not return valid JSON: {result.text}") from exc

    def _normalize_messages(
        self,
        system: str | None,
        prompt: str | None,
        messages: list[dict[str, str]] | None,
    ) -> list[dict[str, str]]:
        if messages:
            normalized = []
            for message in messages:
                role = message["role"]
                content = message["content"]
                normalized.append({"role": role, "content": content})
            return normalized

        if prompt is None:
            raise ValueError("prompt or messages is required")

        normalized = []
        if system:
            normalized.append({"role": "system", "content": system})
        normalized.append({"role": "user", "content": prompt})
        return normalized

    def _call_openai(
        self,
        messages: list[dict[str, str]],
        *,
        response_format: dict[str, Any] | None = None,
    ) -> LLMResult:
        payload: dict[str, Any] = {
            "model": self.config.model,
            "messages": messages,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
        }
        if response_format:
            payload["response_format"] = response_format

        raw = self._post_json(
            url=f"{self.config.base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
            },
            payload=payload,
        )

        try:
            text = raw["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LLMError(f"Unexpected OpenAI response shape: {raw}") from exc

        return LLMResult(
            provider="openai",
            model=raw.get("model", self.config.model),
            text=text or "",
            raw=raw,
            usage=raw.get("usage", {}),
        )

    def _call_anthropic(self, messages: list[dict[str, str]]) -> LLMResult:
        system_parts = [m["content"] for m in messages if m["role"] == "system"]
        chat_messages = [
            {"role": m["role"], "content": m["content"]}
            for m in messages
            if m["role"] != "system"
        ]

        payload: dict[str, Any] = {
            "model": self.config.model,
            "messages": chat_messages,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
        }
        if system_parts:
            payload["system"] = "\n\n".join(system_parts)

        raw = self._post_json(
            url=f"{self.config.base_url}/messages",
            headers={
                "x-api-key": self.config.api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            payload=payload,
        )

        try:
            text = "".join(
                block["text"]
                for block in raw["content"]
                if block.get("type") == "text"
            )
        except (KeyError, TypeError) as exc:
            raise LLMError(f"Unexpected Anthropic response shape: {raw}") from exc

        return LLMResult(
            provider="anthropic",
            model=raw.get("model", self.config.model),
            text=text,
            raw=raw,
            usage=raw.get("usage", {}),
        )

    def _post_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")

        import time
        import random
        max_retries = 3
        body = ""
        for attempt in range(max_retries):
            request = urllib.request.Request(
                url=url,
                data=data,
                headers=headers,
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=self.config.timeout) as response:
                    body = response.read().decode("utf-8")
                break
            except urllib.error.HTTPError as exc:
                if attempt == max_retries - 1 or exc.code not in {429, 500, 502, 503, 504}:
                    error_body = exc.read().decode("utf-8", errors="replace")
                    raise LLMError(
                        f"LLM request failed with HTTP {exc.code}: {error_body}"
                    ) from exc
                time.sleep(2 ** (attempt + 1) + random.uniform(0, 1))
            except urllib.error.URLError as exc:
                if attempt == max_retries - 1:
                    raise LLMError(f"LLM request failed: {exc}") from exc
                time.sleep(2 ** (attempt + 1))

        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise LLMError(f"LLM returned non-JSON body: {body}") from exc
